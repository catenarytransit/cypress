# PR 4 — Selective token retrieval (experimental)

PR 3's `cypress_tokens.bin` is now mmap-loaded and validated together
with the main `CypressMemDb` generation. The token sidecar contains
global multilingual string IDs, not place IDs; the **existing**
string-to-place/street fanout and structured ranker are reused.

## 1. Compiler

Build the compiler and query server:

```sh
cargo build --release --bin compiler --bin query
```

Recompile the dataset from Scylla (same source configuration as before),
**with** the optional token sidecar:

```sh
./target/release/compiler --scylla-url 127.0.0.1 --out-dir ./data/compiled --token-index
```

Rebuild the sidecar whenever the source dictionary changes. Never copy a
sidecar from another dataset. The query server rejects mismatched string
fingerprints during load and reload. The compiler logs the exact size delta.

## 2. Query server / runtime

Use the SAME compiled directory for all modes:

```sh
# Safe production mode, default; ignores token candidates
CYPRESS_TOKEN_RETRIEVAL_MODE=legacy ./target/release/query --memdb-dir ./data/compiled

# Measure selective token plans, but serve unchanged legacy results
RUST_LOG=info,cypress::query::timing=debug CYPRESS_TOKEN_RETRIEVAL_MODE=shadow ./target/release/query --memdb-dir ./data/compiled

# Approximate experimental token path (NOT production-safe yet)
RUST_LOG=info,cypress::query::timing=debug CYPRESS_TOKEN_RETRIEVAL_MODE=token ./target/release/query --memdb-dir ./data/compiled
```

Mode selection is read once per process; **restart** the query binary to
change modes. `shadow` still runs the legacy algorithm and adds token-index
work, so it can be slower than `legacy`. `token` only bypasses bigrams when
all query tokens are present, the rarest posting has at most 50,000 IDs,
and the resulting conjunction has 1–6,000 search keys. Otherwise it runs
the original bigram retrieval unchanged. Prefix candidates are still
unioned and all selected candidates go through the existing place scorer.

IMPORTANT: token conjunction requires the words to appear in the SAME
canonical search string. This is not equivalent to legacy fuzzy retrieval
or to matching `Eiffel Tower Paris` across a POI name and admin area,
or `東京 Station` across distinct aliases. The experimental `token` mode
can omit valid results and must NOT replace production default until
multilingual recall, field-aware candidate joins and fuzzy fallback have
been validated. Global place lookup itself is not country-filtered.

## 3. Compare results

Run separate processes sequentially (or on separate ports). Capture the
legacy ordered result IDs first:

```sh
python3 benchmarks/retrieval_benchmark.py --cycles 11 --warmup 2 --seed 20261015 --output-dir benchmark_results/legacy
```

Switch to `shadow` and repeat; IDs should be unchanged because shadow
always uses legacy ranking. Then switch to `token`:

```sh
python3 benchmarks/retrieval_benchmark.py --cycles 11 --warmup 2 --seed 20261015 --compare-baseline benchmark_results/legacy/baseline.json --output-dir benchmark_results/token
```

Use `--strict-ids` for exact ordered-ID parity; approximate mode is
expected to have some differences and these must be evaluated.

## 4. Observability and algorithmic costs

Set `RUST_LOG=info,cypress::query::timing=debug` for diagnostics. The
`token retrieval plan` log includes token candidates, anchor DF,
`posting_entries_examined`, and `skip_block_seeks`. The existing
`query retrieval baseline` includes `posting_entries_examined` for
actual legacy bigram traversal (zero on a token fast path) and the
counterfactual cold cost for the same query. Do NOT interpret
0 legacy bigram visits as 0 total posting work: account for the
separate compressed token postings, seeks, candidate fanout and CPU time.

The planner looks up each token's document frequency and picks the
smallest posting as its anchor. The anchor is decoded sequentially.
For every anchor ID, it uses each other posting's 128-entry block
directory to seek to the first block whose last ID is >= the target,
then decodes that block only until match/miss. It never scans the
full posting for a common secondary token.

## 5. Checks

```sh
cargo fmt --all -- --check
cargo test --bin query
cargo test --lib token_index
cargo test --bin compiler
cargo check --all-targets
```

One-time startup sidecar validation is O(vocabulary size). Query-time
borrowed views skip that full validation and perform bounded structural
checks and FST lookups. Rebuild compilation generation for any mismatched
sidecar; don't combine index files from separate builds.
