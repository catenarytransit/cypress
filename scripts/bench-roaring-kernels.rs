// Standalone harness, avoiding a release build of the database/network stack.
// rustc --edition=2021 -O --test scripts/bench-roaring-kernels.rs -o /tmp/roaring-kernels
// /tmp/roaring-kernels --ignored --nocapture
#[path = "../src/models/roaring_simd.rs"]
mod roaring_simd;
