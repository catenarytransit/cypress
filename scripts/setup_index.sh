#!/bin/bash
# Setup Elasticsearch Index with Custom Storage Path
#
# Usage: ./setup_index.sh --dir /path/to/data [options]
#
# Options:
#   -d, --dir <path>      Path to directory where Elasticsearch data will be stored (only used if starting local container)
#   -p, --port <port>     Elasticsearch port (default: 9200)
#   -u, --url <url>       Custom Elasticsearch URL (e.g. http://10.0.0.5:9200). 
#                         If provided, skips local Docker container management.
#   --force               Force recreation of the container and index
#
# This script:
# 1. Starts an Elasticsearch Docker container with the specified data volume (unless --url is used)
# 2. Waits for Elasticsearch to become healthy
# 3. Creates the 'places' index with the defined mapping

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
SCHEMA_FILE="${PROJECT_DIR}/schema/places_mapping.json"

# Default values
DATA_DIR=""
ES_PORT=9200
FORCE=false
CONTAINER_NAME="cypress-es"
INDEX_NAME="places"
CUSTOM_URL=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -d|--dir)
            DATA_DIR="$2"
            shift 2
            ;;
        -p|--port)
            ES_PORT="$2"
            shift 2
            ;;
        -u|--url)
            CUSTOM_URL="$2"
            shift 2
            ;;
        --force)
            FORCE=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--dir /path/to/data] [--port 9200] [--url http://...] [--force]"
            exit 1
            ;;
    esac
done

# Set URL and Mode
if [ -n "$CUSTOM_URL" ]; then
    URL="$CUSTOM_URL"
    SKIP_DOCKER=true
    echo "Using custom Elasticsearch URL: $URL"
    echo "Skipping local Docker container management."
else
    # Use environment variable if set, otherwise default
    if [ -n "$ELASTICSEARCH_URL" ]; then
        URL="$ELASTICSEARCH_URL"
        # If user explicitly set env var, maybe they mean remote? 
        # But usually scripts/setup_index.sh is for local setup. 
        # Let's stick to: if --url is passed, skip docker. 
        # If ENV is passed, we might still want to start docker if it points to localhost?
        # To be safe, let's treat ELASTICSEARCH_URL same as --url if strictly provided? 
        # Actually, let's just stick to command line args for explicit control logic.
        # But commonly we might want to respect the env var. 
        # Let's assume if ELASTICSEARCH_URL is different from localhost defaults, maybe we treat it as custom?
        # Simpler: Just use --url for now as requested.
        : # no-op
    fi
    # Default local
    URL="http://localhost:$ES_PORT"
    SKIP_DOCKER=false
fi

# Validate arguments for Docker mode
if [ "$SKIP_DOCKER" = false ]; then
    if [ -z "$DATA_DIR" ]; then
        echo "Error: Data directory is required when managing local container."
        echo "Usage: $0 --dir /path/to/data"
        exit 1
    fi
    
    # Ensure data directory exists
    if [ ! -d "$DATA_DIR" ]; then
        echo "Creating data directory: $DATA_DIR"
        mkdir -p "$DATA_DIR"
    fi

    # Check for Docker
    if ! command -v docker &> /dev/null; then
        echo "Error: docker is not installed or not in PATH."
        exit 1
    fi
fi

echo "=== Cypress Index Setup ==="
echo "Target URL: $URL"
if [ "$SKIP_DOCKER" = false ]; then
    echo "Data Directory: $DATA_DIR"
    echo "Port: $ES_PORT"
fi
echo

# Docker Container Management (only if not skipping)
if [ "$SKIP_DOCKER" = false ]; then
    # Check if container exists
    if [ "$(docker ps -a -q -f name=^/${CONTAINER_NAME}$)" ]; then
        if [ "$FORCE" = true ]; then
            echo "Stopping and removing existing container..."
            docker stop "$CONTAINER_NAME" >/dev/null 2>&1 || true
            docker rm "$CONTAINER_NAME" >/dev/null 2>&1
        else
            echo "Container '$CONTAINER_NAME' already exists."
            if [ "$(docker ps -q -f name=^/${CONTAINER_NAME}$)" ]; then
                echo "Container is running."
            else
                echo "Starting existing container..."
                docker start "$CONTAINER_NAME"
            fi
            echo "Note: Use --force to recreate the container with new settings (e.g. if changing data dir)."
        fi
    else
        FORCE=true # Ensure we fall through to creation logic if it doesn't exist
    fi

    # Start container if we are forcing recreation or it didn't exist
    if [ "$FORCE" = true ]; then
        echo "Starting Elasticsearch container..."
        # Ensure the user has permissions to write to the data dir (ES runs as uid 1000 usually)
        # We purposefully don't change permissions heavily on the host, but be aware of issues.
        # For now, we rely on standard docker behavior.
        
        docker run -d \
            --name "$CONTAINER_NAME" \
            -p "$ES_PORT":9200 \
            -v "$DATA_DIR":/usr/share/elasticsearch/data \
            -e "discovery.type=single-node" \
            -e "xpack.security.enabled=false" \
            -e "ES_JAVA_OPTS=-Xms1g -Xmx1g" \
            docker.elastic.co/elasticsearch/elasticsearch:8.11.0
    fi
fi

# Wait for health
echo "Waiting for Elasticsearch to be ready at $URL ..."
RETRIES=30
COUNT=0

while ! curl -s "$URL/_cat/health" > /dev/null; do
    if [ $COUNT -ge $RETRIES ]; then
        echo "Error: Elasticsearch failed to respond within 60 seconds."
        if [ "$SKIP_DOCKER" = false ]; then
             docker logs "$CONTAINER_NAME" | tail -n 20
        fi
        exit 1
    fi
    sleep 2
    echo -n "."
    COUNT=$((COUNT+1))
done
echo " Ready!"

# Create Index
echo "Setting up '$INDEX_NAME' index..."

# Check if index exists
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -I "$URL/$INDEX_NAME")

if [ "$HTTP_CODE" -eq 200 ]; then
    if [ "$FORCE" = true ]; then
        echo "Deleting existing index..."
        curl -s -X DELETE "$URL/$INDEX_NAME" > /dev/null
    else
        echo "Index '$INDEX_NAME' already exists."
        echo "Setup complete."
        exit 0
    fi
fi

echo "Creating index with mapping from schema..."
if [ ! -f "$SCHEMA_FILE" ]; then
    echo "Error: Schema file not found at $SCHEMA_FILE"
    exit 1
fi

RESPONSE=$(curl -s -X PUT "$URL/$INDEX_NAME" \
    -H 'Content-Type: application/json' \
    -d @"$SCHEMA_FILE")

if echo "$RESPONSE" | grep -q '"acknowledged":true'; then
    echo "Index created successfully."
else
    echo "Error creating index:"
    echo "$RESPONSE"
    exit 1
fi

echo "=== Setup Complete ==="
echo "Elasticsearch is running at $URL"
if [ "$SKIP_DOCKER" = false ]; then
    echo "Data stored in: $DATA_DIR"
fi
