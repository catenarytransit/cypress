/// SIFT4 approximate string distance — ported from adr/sift4.h.
/// Returns a fast approximate edit distance between two byte strings,
/// accounting for transpositions, with early termination at `max_distance`.
use std::cell::Cell;

// Exact Sift4 invocation counts for the synchronous query path. Disabled by
// default: profiling is only enabled when cypress::query::timing is DEBUG.
thread_local! {
    static QUERY_SIFT4_CALLS: Cell<Option<u64>> = const { Cell::new(None) };
}

/// RAII ensures a request cannot leave counters enabled on its worker thread.
/// The synchronous scorer does not yield, so the thread-local counter cannot
/// accidentally include another async request between awaits.
pub struct Sift4CountGuard {
    enabled: bool,
}

impl Sift4CountGuard {
    pub fn new(enabled: bool) -> Self {
        if enabled {
            QUERY_SIFT4_CALLS.with(|calls| calls.set(Some(0)));
        }
        Self { enabled }
    }

    #[inline]
    pub fn count(&self) -> u64 {
        if self.enabled {
            QUERY_SIFT4_CALLS.with(|calls| calls.get().unwrap_or(0))
        } else {
            0
        }
    }
}

impl Drop for Sift4CountGuard {
    fn drop(&mut self) {
        if self.enabled {
            QUERY_SIFT4_CALLS.with(|calls| calls.set(None));
        }
    }
}

#[derive(Clone, Copy)]
pub struct SiftOffset {
    pub c1: usize,
    pub c2: usize,
    pub trans: bool,
}

pub fn sift4(
    s1: &[u8],
    s2: &[u8],
    max_offset: usize,
    max_distance: usize,
    offset_arr: &mut Vec<SiftOffset>,
) -> usize {
    QUERY_SIFT4_CALLS.with(|calls| {
        if let Some(count) = calls.get() {
            calls.set(Some(count.saturating_add(1)));
        }
    });
    offset_arr.clear();

    if s1.is_empty() {
        return s2.len();
    }
    if s2.is_empty() {
        return s1.len();
    }

    let l1 = s1.len();
    let l2 = s2.len();

    let mut c1: usize = 0;
    let mut c2: usize = 0;
    let mut lcss: usize = 0;
    let mut local_cs: usize = 0;
    let mut trans: usize = 0;

    while c1 < l1 && c2 < l2 {
        if s1[c1] == s2[c2] {
            local_cs += 1;
            let mut is_trans = false;

            let mut i = 0;
            while i < offset_arr.len() {
                let ofs = offset_arr[i];
                if c1 <= ofs.c1 || c2 <= ofs.c2 {
                    is_trans = c2.abs_diff(c1) >= ofs.c2.abs_diff(ofs.c1);
                    if is_trans {
                        trans += 1;
                    } else if !ofs.trans {
                        offset_arr[i].trans = true;
                        trans += 1;
                    }
                    break;
                } else if c1 > ofs.c2 && c2 > ofs.c1 {
                    offset_arr.remove(i);
                } else {
                    i += 1;
                }
            }

            offset_arr.push(SiftOffset {
                c1,
                c2,
                trans: is_trans,
            });
        } else {
            lcss += local_cs;
            local_cs = 0;
            if c1 != c2 {
                let m = c1.min(c2);
                c1 = m;
                c2 = m;
            }

            if max_distance > 0 {
                let temp_dist = c1.max(c2) - lcss + trans;
                if temp_dist > max_distance {
                    return temp_dist;
                }
            }

            for i in 1..max_offset {
                if c1 + i >= l1 && c2 + i >= l2 {
                    break;
                }
                if c1 + i < l1 && s1[c1 + i] == s2[c2] {
                    c1 = c1 + i - 1;
                    c2 = c2.wrapping_sub(1);
                    break;
                }
                if c2 + i < l2 && s1[c1] == s2[c2 + i] {
                    c1 = c1.wrapping_sub(1);
                    c2 = c2 + i - 1;
                    break;
                }
            }
        }

        c1 = c1.wrapping_add(1);
        c2 = c2.wrapping_add(1);

        if c1 >= l1 || c2 >= l2 {
            lcss += local_cs;
            local_cs = 0;
            let m = c1.min(c2);
            c1 = m;
            c2 = m;
        }
    }

    lcss += local_cs;
    l1.max(l2) - lcss + trans
}
