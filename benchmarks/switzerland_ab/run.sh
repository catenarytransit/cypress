#!/usr/bin/env bash
# Run from repository root. Works on a provisioned GitHub runner with Docker, Rust, osmium.
set -euo pipefail
ROOT="$(pwd)"
DATA="$ROOT/.swiss-ab-data"
RESULTS="$ROOT/benchmark_results/switzerland_ab"
mkdir -p "$DATA" "$RESULTS" "$DATA/compiled" "$DATA/tmp"
# Cargo's shared target/release/compiler path can serve a baseline executable
# when two worktrees have the same package/bin name. Keep the two build artifacts
# physically isolated, rather than relying on cross-worktree fingerprinting.
# The runner actually reported ~145 GiB disk for the previous full run.
BASE_TARGET="$DATA/target-main"
EXPERIMENT_TARGET="$DATA/target-experiment"
exec > >(tee -a "$RESULTS/run.log") 2>&1

echo "Baseline: cypress/main at workflow start"
echo "Experiment: $(git rev-parse HEAD)"
BASE_SHA="$(git ls-remote https://github.com/catenarytransit/cypress.git refs/heads/main | cut -f1)"
test -n "$BASE_SHA"
git worktree add --detach "$DATA/main" "$BASE_SHA"
trap 'cleanup' EXIT
cleanup() {
  if [[ -n "${SERVER_PID:-}" ]]; then kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; fi
  docker rm -f cypress-swiss-ab-scylla >/dev/null 2>&1 || true
}
# One shared Scylla dataset is imported below, sequentially country by country.
# Do not retain all five raw/filter PBF copies on the 14 GiB hosted runner.
echo "Regions: Switzerland, Germany, France, United Kingdom, Italy (same Scylla DB; distinct A/B compilers)"

if [[ -n "${BENCHMARK_MIN_RAM_GIB:-}" ]]; then
  mem_gib="$(awk '/MemTotal:/ {print int($2 / 1048576)}' /proc/meminfo)"
  if (( mem_gib < BENCHMARK_MIN_RAM_GIB )); then
    echo "Insufficient RAM: ${mem_gib}GiB; need ${BENCHMARK_MIN_RAM_GIB}GiB" >&2
    exit 1
  fi
fi

# Build the binaries before Scylla reserves memory; only ingest and query
# need the database. This avoids Rust builds competing with Scylla on CI.
echo "Building control from pinned main SHA $BASE_SHA"
cargo build --manifest-path "$DATA/main/Cargo.toml" --target-dir "$BASE_TARGET" \
  --locked --release --bin ingest --bin compiler --bin query
cp "$BASE_TARGET/release/query" "$DATA/main-query"
cp "$BASE_TARGET/release/ingest" "$DATA/main-ingest"
cp "$BASE_TARGET/release/compiler" "$DATA/main-compiler"

echo "Building experimental compiler and query from $(git rev-parse HEAD)"
cargo build --manifest-path "$ROOT/Cargo.toml" --target-dir "$EXPERIMENT_TARGET" \
  --locked --release --bin compiler --bin query
cp "$EXPERIMENT_TARGET/release/query" "$DATA/experimental-query"
cp "$EXPERIMENT_TARGET/release/compiler" "$DATA/experimental-compiler"

# Fail before starting Scylla, downloading five country PBFs, or importing.
# This catches accidentally copying main's compiler into the experiment slot.
echo "Verifying experimental compiler CLI and binary provenance"
if ! "$DATA/experimental-compiler" --help | grep -F -- '--roaring-token-index'; then
  echo "ERROR: experiment compiler does not support --roaring-token-index." >&2
  echo "Expected: $ROOT/Cargo.toml at git $(git rev-parse HEAD)" >&2
  echo "Built into: $EXPERIMENT_TARGET/release/compiler" >&2
  exit 1
fi
if "$DATA/main-compiler" --help | grep -Fq -- '--roaring-token-index'; then
  echo "ERROR: baseline binary unexpectedly has the experimental CLI flag." >&2
  exit 1
fi
sha256sum "$DATA/main-compiler" "$DATA/experimental-compiler" \
  | tee "$RESULTS/compiler-executables.sha256"
if cmp -s "$DATA/main-compiler" "$DATA/experimental-compiler"; then
  echo "ERROR: baseline and experimental compiler binaries are byte-identical." >&2
  exit 1
fi
# The compiled executable is now preserved separately from both Cargo outputs.
# Reclaim intermediate build outputs before the geodata import/compilation.
rm -rf "$BASE_TARGET" "$EXPERIMENT_TARGET"
echo "Independent compiler binaries verified. Proceeding with country import."

# Dedicated, disposable Scylla instance. Never point fixture ingest at production.
# Seastar subtracts a default OS reservation (at least ~1.5 GiB) from the
# *container* memory limit before checking the configured --memory heap.
# A 2 GiB Docker cap and --memory 1536M therefore left only 500 MB usable,
# even on a runner with 14 GiB available. The public ubuntu-latest runner has
# 16 GiB RAM, so use 3072 MiB for Scylla and a 5632 MiB Docker cap by default.
# This leaves memory for the import/compiler/query processes outside Docker.
SCYLLA_MEMORY_MIB="${BENCHMARK_SCYLLA_MEMORY_MIB:-3072}"
if [[ ! "$SCYLLA_MEMORY_MIB" =~ ^[0-9]+$ ]] || (( SCYLLA_MEMORY_MIB < 1024 )); then
  echo "BENCHMARK_SCYLLA_MEMORY_MIB must be an integer >= 1024" >&2
  exit 1
fi
# Allow Seastar's implicit 1.5 GiB reservation plus 1 GiB operational headroom.
# Do not equate the Docker cgroup hard limit with Seastar's requested heap.
SCYLLA_SEASTAR_RESERVE_MIB=1536
SCYLLA_CONTAINER_HEADROOM_MIB=1024
SCYLLA_DOCKER_LIMIT_MIB=$(( SCYLLA_MEMORY_MIB + SCYLLA_SEASTAR_RESERVE_MIB + SCYLLA_CONTAINER_HEADROOM_MIB ))
echo "Scylla: ${SCYLLA_MEMORY_MIB} MiB heap, ${SCYLLA_DOCKER_LIMIT_MIB} MiB Docker limit, 2 shards"
echo "Including ${SCYLLA_SEASTAR_RESERVE_MIB} MiB Seastar OS reserve and ${SCYLLA_CONTAINER_HEADROOM_MIB} MiB headroom."
free -h || true
df -h "$ROOT" || true
docker run -d --name cypress-swiss-ab-scylla --memory="${SCYLLA_DOCKER_LIMIT_MIB}m" \
  -p 127.0.0.1:9042:9042 scylladb/scylla:5.4 \
  --smp 2 --memory "${SCYLLA_MEMORY_MIB}M" --overprovisioned 1

wait_for_scylla() {
  local SCYLLA_READY=false
  for i in $(seq 1 120); do
    if docker exec cypress-swiss-ab-scylla cqlsh -e 'DESCRIBE KEYSPACES' >/dev/null 2>&1; then
      SCYLLA_READY=true
      break
    fi
    if docker logs cypress-swiss-ab-scylla 2>&1 | grep -Eq 'insufficient physical memory|gave up: scylla entered FATAL state'; then
      echo "Scylla failed startup. Check Docker cgroup memory and Seastar's OS reserve." >&2
      echo "Docker cap: ${SCYLLA_DOCKER_LIMIT_MIB} MiB; Scylla heap: ${SCYLLA_MEMORY_MIB} MiB." >&2
      echo "Runner memory (Docker may have a separate cgroup limit):" >&2
      free -h >&2 || true
      docker inspect --format 'Docker memory limit: {{.HostConfig.Memory}} bytes' cypress-swiss-ab-scylla >&2 || true
      docker logs --tail 100 cypress-swiss-ab-scylla
      exit 1
    fi
    sleep 5
  done
  if [[ "$SCYLLA_READY" != true ]]; then
    echo "Scylla did not become CQL-ready within 10 minutes" >&2
    docker logs --tail 100 cypress-swiss-ab-scylla
    exit 1
  fi

}
wait_for_scylla


# One country at a time: fetch, filter, ingest, delete all PBF intermediates.
# Only the first import truncates the disposable Scylla tables; subsequent
# imports upsert into the *same* complete five-country dataset.
import_region() {
  local region="$1" truncate="$2"
  local source="$DATA/${region}.osm.pbf"
  local filtered="$DATA/${region}-filtered.osm.pbf"
  local admins="$DATA/${region}-admins.osm.pbf"
  local url="https://download.geofabrik.de/europe/${region}-latest.osm.pbf"
  echo "Importing ${region}: ${url}"
  curl --fail --location --retry 4 --retry-delay 5 --output "$source" "$url"
  test -s "$source"
  sha256sum "$source" | tee "$RESULTS/${region}.sha256"
  osmium fileinfo "$source" > "$RESULTS/${region}-pbf-info.txt"
  "$ROOT/scripts/filter_osm.sh" "$source" "$filtered"
  "$ROOT/scripts/filter_admins.sh" "$source" "$admins"
  rm -f "$source"
  if [[ "$truncate" == "yes" ]]; then
    time "$DATA/main-ingest" single --file "$filtered" --admin-file "$admins" \
      --scylla-url 127.0.0.1 --create-index
  else
    time "$DATA/main-ingest" single --file "$filtered" --admin-file "$admins" \
      --scylla-url 127.0.0.1
  fi
  rm -f "$filtered" "$admins"
  df -h "$DATA"
}
import_region switzerland yes
import_region germany no
import_region france no
import_region united-kingdom no
import_region italy no
echo "All five regions imported into one disposable Scylla database"
docker stats --no-stream cypress-swiss-ab-scylla > "$RESULTS/scylla-after-import.txt"

# Two independently compiled generations from exactly the same source records,
# but distinct compilers / index layouts. Compile and benchmark A first, then
# reclaim its generation before building B (the same Scylla DB remains alive).
echo "Compiling baseline main token index"
time "$DATA/main-compiler" --scylla-url 127.0.0.1 \
  --out-dir "$DATA/compiled/baseline" --tmp-dir "$DATA/tmp/baseline" \
  --concurrency "${BENCHMARK_COMPILER_CONCURRENCY:-2}" --token-index
du -sh "$DATA/compiled/baseline" | tee "$RESULTS/index-size-baseline.txt"
find "$DATA/compiled/baseline" -type f -printf '%P %s bytes\n' | sort > "$RESULTS/index-files-baseline.txt"

# Identical production flags for both binaries: only the retrieval selector changes.
export RUST_LOG='debug,cypress=debug,cypress::query::timing=debug'
export CYPRESS_AREA_FANOUT=on
export CYPRESS_CANDIDATE_SHORTLIST=on
export CYPRESS_CANDIDATE_BUDGET=256

# Capture resident file pages separately from heap/cache allocations. These are
# snapshots, not peak RSS; faults and I/O help diagnose the larger working set.
snapshot_engine() {
  local name="$1" stage="$2"
  ps -o pid=,rss=,vsz=,etime= -p "$SERVER_PID" > "$RESULTS/$name-process-$stage.txt"
  cat "/proc/$SERVER_PID/status" > "$RESULTS/$name-status-$stage.txt"
  cat "/proc/$SERVER_PID/smaps_rollup" > "$RESULTS/$name-smaps-$stage.txt"
  cat "/proc/$SERVER_PID/stat" > "$RESULTS/$name-stat-$stage.txt"
  cat "/proc/$SERVER_PID/io" > "$RESULTS/$name-io-$stage.txt"
  cat /proc/meminfo > "$RESULTS/$name-host-memory-$stage.txt"
  docker stats --no-stream cypress-swiss-ab-scylla > "$RESULTS/$name-scylla-$stage.txt"
}

run_engine() {
  local name="$1" binary="$2" mode="$3"
  export CYPRESS_TOKEN_RETRIEVAL_MODE="$mode"
  echo "Restarting disposable Scylla after $name compilation, before queries"
  docker exec cypress-swiss-ab-scylla nodetool flush
  docker restart --time 120 cypress-swiss-ab-scylla
  wait_for_scylla
  echo "Starting $name, retrieval=$mode"
  "$binary" --memdb-dir "$DATA/compiled/$name" --scylla-url 127.0.0.1 \
    --listen 127.0.0.1:3000 > "$RESULTS/$name-server.log" 2>&1 &
  SERVER_PID=$!
  local ready=false
  for i in $(seq 1 120); do
    if curl -fsS http://127.0.0.1:3000/health >/dev/null 2>&1; then ready=true; break; fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then break; fi
    sleep 1
  done
  if [[ "$ready" != true ]]; then
    tail -n 100 "$RESULTS/$name-server.log"
    exit 1
  fi
  snapshot_engine "$name" start
  # Preserve a separate first pass before the unchanged warm benchmark. This
  # resets Scylla process caches, but does not claim a cold Linux page cache.
  python3 benchmarks/switzerland_ab/compare.py measure \
    --label "$name" --url http://127.0.0.1:3000 \
    --workload benchmarks/switzerland_ab/workload.jsonl \
    --output "$RESULTS/$name-first-pass.json" --cycles 1 --warmup 0
  snapshot_engine "$name" first-pass
  python3 benchmarks/switzerland_ab/compare.py measure \
    --label "$name" --url http://127.0.0.1:3000 \
    --workload benchmarks/switzerland_ab/workload.jsonl \
    --output "$RESULTS/$name.json" --cycles "${BENCHMARK_CYCLES:-11}" --warmup 2
  snapshot_engine "$name" end
  kill "$SERVER_PID"
  wait "$SERVER_PID" 2>/dev/null || true
  SERVER_PID=""
  sleep 3
}
# First run control, then experiment; repeat in reverse order on dedicated hosts
# for more robust comparisons of memory cache and thermal effects.
run_engine baseline "$DATA/main-query" token
echo "Reclaiming baseline mmap files before experimental compile"
rm -rf "$DATA/compiled/baseline"
df -h "$DATA"
echo "Compiling Roaring-style hybrid posting index using its own compiler"
time "$DATA/experimental-compiler" --scylla-url 127.0.0.1 \
  --out-dir "$DATA/compiled/experiment" --tmp-dir "$DATA/tmp/experiment" \
  --concurrency "${BENCHMARK_COMPILER_CONCURRENCY:-2}" \
  --roaring-token-index
du -sh "$DATA/compiled/experiment" | tee "$RESULTS/index-size-experiment.txt"
find "$DATA/compiled/experiment" -type f -printf '%P %s bytes\n' | sort > "$RESULTS/index-files-experiment.txt"
test -s "$DATA/compiled/experiment/cypress_roaring_tokens.bin"
run_engine experiment "$DATA/experimental-query" roaring
python3 benchmarks/switzerland_ab/analyze_logs.py "$RESULTS"
python3 benchmarks/switzerland_ab/compare.py summarize \
  --baseline "$RESULTS/baseline.json" \
  --experiment "$RESULTS/experiment.json" --output "$RESULTS"
git rev-parse HEAD > "$RESULTS/experiment.sha"
printf '%s\n' "switzerland germany france united-kingdom italy" > "$RESULTS/imported-regions.txt"
printf '%s\n' "$BASE_SHA" > "$RESULTS/main.sha"
docker stats --no-stream cypress-swiss-ab-scylla > "$RESULTS/scylla-memory.txt"
echo "Saved summary: $RESULTS/summary.md"
