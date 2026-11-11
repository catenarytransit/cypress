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

# Dedicated, disposable Scylla instance. Never point the fixture ingest at production.
docker run -d --name cypress-swiss-ab-scylla --memory=4g \
  -p 127.0.0.1:9042:9042 scylladb/scylla:5.4 \
  --smp 2 --memory 3G --overprovisioned 1
for i in $(seq 1 120); do
  if docker exec cypress-swiss-ab-scylla cqlsh -e 'DESCRIBE KEYSPACES' >/dev/null 2>&1; then
    break
  fi
  if (( i == 120 )); then docker logs cypress-swiss-ab-scylla; exit 1; fi
  sleep 5
done

echo "Building control from pinned main SHA $BASE_SHA"
( cd "$DATA/main" && cargo build --locked --release --bin ingest --bin compiler --bin query )
cp "$DATA/main/target/release/query" "$DATA/main-query"
cp "$DATA/main/target/release/ingest" "$DATA/main-ingest"
cp "$DATA/main/target/release/compiler" "$DATA/main-compiler"
echo "Building experimental query binary"
cargo build --locked --release --bin query
cp target/release/query "$DATA/experimental-query"

echo "Importing Switzerland once from identical unfiltered-source subset"
time "$DATA/main-ingest" single \
  --file "$DATA/switzerland-filtered.osm.pbf" \
  --admin-file "$DATA/switzerland-admins.osm.pbf" \
  --scylla-url 127.0.0.1 --create-index
echo "Compiling common generation with token FST/postings enabled"
time "$DATA/main-compiler" --scylla-url 127.0.0.1 \
  --out-dir "$DATA/compiled" --tmp-dir "$DATA/tmp" \
  --concurrency 4 --token-index
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
