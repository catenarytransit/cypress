#!/usr/bin/env bash
# Run from repository root. Works on a provisioned GitHub runner with Docker, Rust, osmium.
set -euo pipefail
ROOT="$(pwd)"
DATA="$ROOT/.swiss-ab-data"
RESULTS="$ROOT/benchmark_results/switzerland_ab"
mkdir -p "$DATA" "$RESULTS" "$DATA/compiled" "$DATA/tmp"
# Share Cargo dependencies between the pinned-main and experimental worktrees
# to keep two release builds within the standard public runner disk budget.
export CARGO_TARGET_DIR="$DATA/cargo-target"
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
# Do not retain all three raw/filter PBF copies on the 14 GiB hosted runner.
echo "Regions: Switzerland, Germany, France (same Scylla DB; distinct A/B compilers)"

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
( cd "$DATA/main" && cargo build --locked --release --bin ingest --bin compiler --bin query )
cp "$CARGO_TARGET_DIR/release/query" "$DATA/main-query"
cp "$CARGO_TARGET_DIR/release/ingest" "$DATA/main-ingest"
cp "$CARGO_TARGET_DIR/release/compiler" "$DATA/main-compiler"
echo "Building experimental compiler and query binaries"
cargo build --locked --release --bin compiler --bin query
cp "$CARGO_TARGET_DIR/release/query" "$DATA/experimental-query"
cp "$CARGO_TARGET_DIR/release/compiler" "$DATA/experimental-compiler"

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

SCYLLA_READY=false
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


# One country at a time: fetch, filter, ingest, delete all PBF intermediates.
# Only the first import truncates the disposable Scylla tables; subsequent
# imports upsert into the *same* complete three-country dataset.
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
echo "All three regions imported into one disposable Scylla database"
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

run_engine() {
  local name="$1" binary="$2" mode="$3"
  export CYPRESS_TOKEN_RETRIEVAL_MODE="$mode"
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
  ps -o pid=,rss=,vsz=,etime= -p "$SERVER_PID" > "$RESULTS/$name-process-start.txt"
  python3 benchmarks/switzerland_ab/compare.py measure \
    --label "$name" --url http://127.0.0.1:3000 \
    --workload benchmarks/switzerland_ab/workload.jsonl \
    --output "$RESULTS/$name.json" --cycles "${BENCHMARK_CYCLES:-11}" --warmup 2
  ps -o pid=,rss=,vsz=,etime= -p "$SERVER_PID" > "$RESULTS/$name-process-end.txt"
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
echo "Compiling experimental packed posting index using its own compiler"
time "$DATA/experimental-compiler" --scylla-url 127.0.0.1 \
  --out-dir "$DATA/compiled/experiment" --tmp-dir "$DATA/tmp/experiment" \
  --concurrency "${BENCHMARK_COMPILER_CONCURRENCY:-2}" \
  --token-index --packed-token-index
du -sh "$DATA/compiled/experiment" | tee "$RESULTS/index-size-experiment.txt"
find "$DATA/compiled/experiment" -type f -printf '%P %s bytes\n' | sort > "$RESULTS/index-files-experiment.txt"
test -s "$DATA/compiled/experiment/cypress_token_pack.bin"
run_engine experiment "$DATA/experimental-query" open-geocode
python3 benchmarks/switzerland_ab/compare.py summarize \
  --baseline "$RESULTS/baseline.json" \
  --experiment "$RESULTS/experiment.json" --output "$RESULTS"
git rev-parse HEAD > "$RESULTS/experiment.sha"\nprintf '%s\\n' "switzerland germany france" > "$RESULTS/imported-regions.txt"
printf '%s\n' "$BASE_SHA" > "$RESULTS/main.sha"
docker stats --no-stream cypress-swiss-ab-scylla > "$RESULTS/scylla-memory.txt"
echo "Saved summary: $RESULTS/summary.md"
