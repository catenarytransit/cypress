#!/usr/bin/env bash
# Run from repository root. Works on a provisioned GitHub runner with Docker, Rust, osmium.
set -euo pipefail
ROOT="$(pwd)"
DATA="$ROOT/.swiss-ab-data"
RESULTS="$ROOT/benchmark_results/switzerland_ab"
mkdir -p "$DATA" "$RESULTS" "$DATA/compiled" "$DATA/tmp"
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
PBF="$DATA/switzerland.osm.pbf"
curl --fail --location --retry 4 --retry-delay 5 \
  --output "$PBF" "https://download.geofabrik.de/europe/switzerland-latest.osm.pbf"
test -s "$PBF"
sha256sum "$PBF" | tee "$RESULTS/geofabrik.sha256"
osmium fileinfo "$PBF" | tee "$RESULTS/geofabrik-info.txt"
"$ROOT/scripts/filter_osm.sh" "$PBF" "$DATA/switzerland-filtered.osm.pbf"
"$ROOT/scripts/filter_admins.sh" "$PBF" "$DATA/switzerland-admins.osm.pbf"

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
cp "$DATA/main/target/release/query" "$DATA/main-query"
cp "$DATA/main/target/release/ingest" "$DATA/main-ingest"
cp "$DATA/main/target/release/compiler" "$DATA/main-compiler"
echo "Building experimental query binary"
cargo build --locked --release --bin query
cp target/release/query "$DATA/experimental-query"

# Dedicated, disposable Scylla instance. Never point fixture ingest at production.
# Seastar subtracts a default OS reservation (at least ~1.5 GiB) from the
# *container* memory limit before checking the configured --memory heap.
# A 2 GiB Docker cap and --memory 1536M therefore left only 500 MB usable,
# even on a runner with 14 GiB available. Keep an additional 1 GiB of headroom
# beyond that reservation; for 1536 MiB Scylla this yields a 4 GiB Docker cap.
# Larger dedicated runners can override BENCHMARK_SCYLLA_MEMORY_MIB.
SCYLLA_MEMORY_MIB="${BENCHMARK_SCYLLA_MEMORY_MIB:-1536}"
if [[ ! "$SCYLLA_MEMORY_MIB" =~ ^[0-9]+$ ]] || (( SCYLLA_MEMORY_MIB < 1024 )); then
  echo "BENCHMARK_SCYLLA_MEMORY_MIB must be an integer >= 1024" >&2
  exit 1
fi
# Allow Seastar's implicit 1.5 GiB reservation plus 1 GiB operational headroom.
# Do not equate the Docker cgroup hard limit with Seastar's requested heap.
SCYLLA_SEASTAR_RESERVE_MIB=1536
SCYLLA_CONTAINER_HEADROOM_MIB=1024
SCYLLA_DOCKER_LIMIT_MIB=$(( SCYLLA_MEMORY_MIB + SCYLLA_SEASTAR_RESERVE_MIB + SCYLLA_CONTAINER_HEADROOM_MIB ))
echo "Scylla: ${SCYLLA_MEMORY_MIB} MiB heap, ${SCYLLA_DOCKER_LIMIT_MIB} MiB Docker limit, 1 shard"
echo "Including ${SCYLLA_SEASTAR_RESERVE_MIB} MiB Seastar OS reserve and ${SCYLLA_CONTAINER_HEADROOM_MIB} MiB headroom."
free -h || true
docker run -d --name cypress-swiss-ab-scylla --memory="${SCYLLA_DOCKER_LIMIT_MIB}m" \
  -p 127.0.0.1:9042:9042 scylladb/scylla:5.4 \
  --smp 1 --memory "${SCYLLA_MEMORY_MIB}M" --overprovisioned 1

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


echo "Importing Switzerland once from identical unfiltered-source subset"
time "$DATA/main-ingest" single \
  --file "$DATA/switzerland-filtered.osm.pbf" \
  --admin-file "$DATA/switzerland-admins.osm.pbf" \
  --scylla-url 127.0.0.1 --create-index
echo "Compiling common generation with token FST/postings enabled"
time "$DATA/main-compiler" --scylla-url 127.0.0.1 \
  --out-dir "$DATA/compiled" --tmp-dir "$DATA/tmp" \
  --concurrency "${BENCHMARK_COMPILER_CONCURRENCY:-2}" --token-index
du -sh "$DATA/compiled" | tee "$RESULTS/index-size.txt"
find "$DATA/compiled" -type f -printf '%P %s bytes\n' | sort > "$RESULTS/index-files.txt"

# Identical production flags for both binaries: only the retrieval selector changes.
export RUST_LOG='debug,cypress=debug,cypress::query::timing=debug'
export CYPRESS_AREA_FANOUT=on
export CYPRESS_CANDIDATE_SHORTLIST=on
export CYPRESS_CANDIDATE_BUDGET=256

run_engine() {
  local name="$1" binary="$2" mode="$3"
  export CYPRESS_TOKEN_RETRIEVAL_MODE="$mode"
  echo "Starting $name, retrieval=$mode"
  "$binary" --memdb-dir "$DATA/compiled" --scylla-url 127.0.0.1 \
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
run_engine experiment "$DATA/experimental-query" open-geocode
python3 benchmarks/switzerland_ab/compare.py summarize \
  --baseline "$RESULTS/baseline.json" \
  --experiment "$RESULTS/experiment.json" --output "$RESULTS"
git rev-parse HEAD > "$RESULTS/experiment.sha"
printf '%s\n' "$BASE_SHA" > "$RESULTS/main.sha"
docker stats --no-stream cypress-swiss-ab-scylla > "$RESULTS/scylla-memory.txt"
echo "Saved summary: $RESULTS/summary.md"
