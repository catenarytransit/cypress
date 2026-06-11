#!/bin/bash
# Migrate version history from Elasticsearch to ScyllaDB
#
# Usage: ./migrate_versions.sh [options]
#   --es-url <url>      Elasticsearch URL (default: http://localhost:9200)
#   --scylla-host <ip>  ScyllaDB Host (default: 127.0.0.1)
#   --scylla-port <p>   ScyllaDB Port (default: 9042)

set -e

ES_URL="http://localhost:9200"
SCYLLA_HOST="127.0.0.1"
SCYLLA_PORT="9042"

# Parse arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --es-url)
            ES_URL="$2"
            shift 2
            ;;
        --scylla-host)
            SCYLLA_HOST="$2"
            shift 2
            ;;
        --scylla-port)
            SCYLLA_PORT="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--es-url http://...] [--scylla-host 127.0.0.1] [--scylla-port 9042]"
            exit 1
            ;;
    esac
done

echo "=== Cypress Version Migration ==="
echo "Elasticsearch URL: $ES_URL"
echo "ScyllaDB Host: $SCYLLA_HOST"
echo "ScyllaDB Port: $SCYLLA_PORT"
echo

if ! command -v jq &> /dev/null; then
    echo "ERROR: 'jq' tool is required to run this migration."
    exit 1
fi

if ! command -v curl &> /dev/null; then
    echo "ERROR: 'curl' tool is required to run this migration."
    exit 1
fi

# Fetch versions from Elasticsearch
echo "Fetching cypress_versions from Elasticsearch..."
RESPONSE=$(curl -s -S "$ES_URL/cypress_versions/_search?size=1000" || true)

if [ -z "$RESPONSE" ]; then
    echo "Could not reach Elasticsearch or get response."
    exit 1
fi

HITS=$(echo "$RESPONSE" | jq -c '.hits.hits[]._source' 2>/dev/null || true)

if [ -n "$HITS" ]; then
    # Ensure keyspace and table exist in ScyllaDB by running a DESCRIBE or simple query
    echo "Ensuring cypress_versions table exists in ScyllaDB..."
    cqlsh $SCYLLA_HOST $SCYLLA_PORT -e "CREATE KEYSPACE IF NOT EXISTS cypress WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };" >/dev/null
    cqlsh $SCYLLA_HOST $SCYLLA_PORT -e "CREATE TABLE IF NOT EXISTS cypress.cypress_versions (region_name text, filename text, hash text, timestamp timestamp, PRIMARY KEY ((region_name, filename), hash));" >/dev/null

    echo "Found versions to migrate. Inserting into ScyllaDB..."
    COUNT=0
    while read -r row; do
        if [ -n "$row" ]; then
            REGION=$(echo "$row" | jq -r '.region_name')
            FILENAME=$(echo "$row" | jq -r '.filename')
            HASH=$(echo "$row" | jq -r '.hash')
            TIMESTAMP=$(echo "$row" | jq -r '.timestamp')

            # Perform insert
            cqlsh $SCYLLA_HOST $SCYLLA_PORT -e "INSERT INTO cypress.cypress_versions (region_name, filename, hash, timestamp) VALUES ('$REGION', '$FILENAME', '$HASH', '$TIMESTAMP');" >/dev/null
            COUNT=$((COUNT + 1))
        fi
    done <<< "$HITS"
    echo "Migration complete. Successfully migrated $COUNT versions."
else
    echo "No Elasticsearch versions found or index does not exist."
fi
