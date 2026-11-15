//! Sorted array probing over unaligned little-endian mmap bytes.
//! V1-style broadcast comparisons, with exponential block skipping for gaps.
//! No typed references into mmap storage and no loads beyond the slice.

#[derive(Clone, Copy)]
pub(super) struct Kernel {
    avx2: bool,
}

impl Kernel {
    pub(super) fn detect() -> Self {
        Self {
            #[cfg(target_arch = "x86_64")]
            avx2: std::arch::is_x86_feature_detected!("avx2"),
            #[cfg(not(target_arch = "x86_64"))]
            avx2: false,
        }
    }

    /// Candidates must increase. `cursor` is private to this array/container.
    pub(super) fn contains<const WIDTH: usize>(
        self,
        bytes: &[u8],
        cursor: &mut usize,
        value: u32,
    ) -> bool {
        let count = bytes.len() / WIDTH;
        let lanes = if self.avx2 { 32 / WIDTH } else { 1 };
        let blocks = count / lanes;
        let mut block = *cursor / lanes;
        // Exponential search in block maxima avoids scanning a broad posting
        // for a tiny anchor. Preserve the current block for adjacent candidates.
        if block < blocks && read::<WIDTH>(bytes, (block + 1) * lanes - 1) < value {
            let mut step = 1usize;
            let mut lower = block + 1;
            while block + step < blocks
                && read::<WIDTH>(bytes, (block + step + 1) * lanes - 1) < value
            {
                lower = block + step + 1;
                step *= 2;
            }
            let mut upper = (block + step + 1).min(blocks);
            while lower < upper {
                let mid = lower + (upper - lower) / 2;
                if read::<WIDTH>(bytes, (mid + 1) * lanes - 1) < value {
                    lower = mid + 1;
                } else {
                    upper = mid;
                }
            }
            block = lower;
        }
        *cursor = block * lanes;
        if block < blocks {
            #[cfg(target_arch = "x86_64")]
            if self.avx2 {
                // SAFETY: runtime AVX2 detection; a complete 32-byte block is
                // within bytes. loadu accepts every mmap alignment.
                return unsafe { equal_block::<WIDTH>(&bytes[*cursor * WIDTH..], value) };
            }
            return read::<WIDTH>(bytes, *cursor) == value;
        }
        while *cursor < count && read::<WIDTH>(bytes, *cursor) < value {
            *cursor += 1;
        }
        *cursor < count && read::<WIDTH>(bytes, *cursor) == value
    }
}

/// Intersect a dense container directly; decode only surviving bits.
pub(super) fn intersect_bitmaps(a: &[u8], b: &[u8], high: u32, out: &mut Vec<u32>) {
    debug_assert_eq!(a.len(), 8192);
    debug_assert_eq!(b.len(), 8192);
    #[cfg(target_arch = "x86_64")]
    if Kernel::detect().avx2 {
        // SAFETY: feature checked; both container bodies are exactly 8192 bytes.
        unsafe {
            intersect_bitmaps_avx2(a, b, high, out);
        }
        return;
    }
    intersect_bitmaps_scalar(a, b, high, out);
}

fn intersect_bitmaps_scalar(a: &[u8], b: &[u8], high: u32, out: &mut Vec<u32>) {
    for i in (0..8192).step_by(8) {
        let bits = u64::from_le_bytes(a[i..i + 8].try_into().unwrap())
            & u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
        emit(bits, high | (i as u32 * 8), out);
    }
}

fn emit(mut bits: u64, base: u32, out: &mut Vec<u32>) {
    while bits != 0 {
        out.push(base | bits.trailing_zeros());
        bits &= bits - 1;
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn intersect_bitmaps_avx2(a: &[u8], b: &[u8], high: u32, out: &mut Vec<u32>) {
    use std::arch::x86_64::*;
    for i in (0..8192).step_by(32) {
        let bits = _mm256_and_si256(
            _mm256_loadu_si256(a.as_ptr().add(i).cast()),
            _mm256_loadu_si256(b.as_ptr().add(i).cast()),
        );
        // Avoid spilling and decoding empty vectors.
        if _mm256_testz_si256(bits, bits) != 0 {
            continue;
        }
        let mut words = [0u64; 4];
        _mm256_storeu_si256(words.as_mut_ptr().cast(), bits);
        for (lane, word) in words.into_iter().enumerate() {
            emit(word, high | ((i + lane * 8) as u32 * 8), out);
        }
    }
}

fn read<const WIDTH: usize>(bytes: &[u8], index: usize) -> u32 {
    let at = index * WIDTH;
    if WIDTH == 2 {
        u16::from_le_bytes(bytes[at..at + 2].try_into().unwrap()) as u32
    } else {
        u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap())
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn equal_block<const WIDTH: usize>(bytes: &[u8], value: u32) -> bool {
    use std::arch::x86_64::*;
    let block = _mm256_loadu_si256(bytes.as_ptr().cast());
    let equal = if WIDTH == 2 {
        _mm256_cmpeq_epi16(block, _mm256_set1_epi16(value as i16))
    } else {
        _mm256_cmpeq_epi32(block, _mm256_set1_epi32(value as i32))
    };
    _mm256_movemask_epi8(equal) != 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check<const WIDTH: usize>(values: &[u32], queries: &[u32]) {
        let mut storage = vec![0u8]; // Deliberately unaligned.
        for v in values {
            storage.extend_from_slice(&v.to_le_bytes()[..WIDTH]);
        }
        for kernel in [Kernel { avx2: false }, Kernel::detect()] {
            let mut cursor = 0;
            for &q in queries {
                assert_eq!(
                    kernel.contains::<WIDTH>(&storage[1..], &mut cursor, q),
                    values.binary_search(&q).is_ok(),
                    "width={WIDTH}, query={q}"
                );
            }
        }
    }

    #[test]
    fn dense_intersection_matches_bit_oracle() {
        let mut a = vec![0u8; 8193];
        let mut b = vec![0u8; 8193];
        for i in 0..65536usize {
            if i % 3 == 0 {
                a[1 + i / 8] |= 1 << (i % 8);
            }
            if i % 5 == 0 {
                b[1 + i / 8] |= 1 << (i % 8);
            }
        }
        let expected: Vec<_> = (0..65536u32)
            .filter(|i| i % 15 == 0)
            .map(|i| 0xffff0000 | i)
            .collect();
        let mut out = Vec::new();
        intersect_bitmaps(&a[1..], &b[1..], 0xffff0000, &mut out);
        assert_eq!(out, expected);
        out.clear();
        intersect_bitmaps_scalar(&a[1..], &b[1..], 0xffff0000, &mut out);
        assert_eq!(out, expected);
        out.clear();
        intersect_bitmaps(&a[1..], &[0; 8192], 0, &mut out);
        assert!(out.is_empty());
    }

    #[test]
    #[ignore = "manual kernel timing; no end-to-end speedup claim"]
    fn benchmark_array_probes() {
        use std::{hint::black_box, time::Instant};
        eprintln!("AVX2: {}", Kernel::detect().avx2);
        for (label, step) in [("balanced", 3), ("skewed", 127)] {
            let values: Vec<u32> = (0..65536u32).step_by(17).collect();
            let mut bytes = Vec::new();
            for &v in &values {
                bytes.extend_from_slice(&(v as u16).to_le_bytes());
            }
            let queries: Vec<u32> = (0..65536u32).step_by(step).collect();
            let start = Instant::now();
            for _ in 0..200 {
                let mut hits = 0;
                for &q in black_box(&queries) {
                    hits += black_box(&values).binary_search(&q).is_ok() as usize;
                }
                black_box(hits);
            }
            let binary = start.elapsed();
            let start = Instant::now();
            for _ in 0..200 {
                let mut cursor = 0;
                let mut hits = 0;
                for &q in black_box(&queries) {
                    hits +=
                        Kernel::detect().contains::<2>(black_box(&bytes), &mut cursor, q) as usize;
                }
                black_box(hits);
            }
            eprintln!(
                "{label}: binary={binary:?}, dispatched={:?}",
                start.elapsed()
            );
        }
    }

    #[test]
    fn tails_gaps_and_unsigned_boundaries() {
        for len in 0..130 {
            let values: Vec<u32> = (0..len).map(|x| x * 3).collect();
            let queries: Vec<u32> = (0..400).collect();
            check::<2>(&values, &queries);
            check::<4>(&values, &queries);
        }
        let values: Vec<_> = (0..65536u32).step_by(3).collect();
        let queries = [0, 1, 32767, 32768, 32769, 65000, 65534, 65535];
        check::<2>(&values, &queries);
        let values: Vec<_> = (0..1024).map(|i| u32::MAX - 4096 + i * 4).collect();
        check::<4>(
            &values,
            &[0, 2147483648, u32::MAX - 4096, u32::MAX - 4, u32::MAX],
        );
    }

    #[test]
    fn randomized_monotone_probes() {
        let mut seed = 17u32;
        for _ in 0..100 {
            let mut values = Vec::new();
            let mut queries = Vec::new();
            for i in 0..65536u32 {
                seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
                if seed >> 27 == 0 {
                    values.push(i);
                }
                if seed >> 28 == 1 {
                    queries.push(i);
                }
            }
            check::<2>(&values, &queries);
            check::<4>(&values, &queries);
        }
    }
}
