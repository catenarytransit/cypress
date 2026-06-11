#!/bin/bash
# Setup ScyllaDB Container
#
# Usage: ./setup_index.sh --dir /path/to/data [options]
#
# Options:
#   -d, --dir <path>      Path to directory where ScyllaDB data will be stored (only used if starting local container)
#   -p, --port <port>     ScyllaDB CQL port (default: 9042)
#   -u, --host <host>     Custom ScyllaDB host (default: 127.0.0.1).
#                         If provided, skips local Docker container management.
#   --force               Force recreation of the container
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Default values
DATA_DIR=""
SCYLLA_PORT=9042
FORCE=false
CONTAINER_NAME="cypress-scylla"
CUSTOM_HOST=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -d|--dir)
            DATA_DIR="$2"
            shift 2
            ;;
        -p|--port)
            SCYLLA_PORT="$2"
            shift 2
            ;;
        -u|--host)
            CUSTOM_HOST="$2"
            shift 2
            ;;
        --force)
            FORCE=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--dir /path/to/data] [--port 9042] [--host 127.0.0.1] [--force]"
            exit 1
            ;;
    esac
done

if [ -n "$CUSTOM_HOST" ]; then
    HOST="$CUSTOM_HOST"
    SKIP_DOCKER=true
    echo "Using custom ScyllaDB host: $HOST"
    echo "Skipping local Docker container management."
else
    HOST="127.0.0.1"
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

echo "=== Cypress ScyllaDB Setup ==="
echo "Target Host: $HOST"
if [ "$SKIP_DOCKER" = false ]; then
    echo "Data Directory: $DATA_DIR"
    echo "Port: $SCYLLA_PORT"
fi
echo

# Docker Container Management
if [ "$SKIP_DOCKER" = false ]; then
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
        fi
    else
        FORCE=true
    fi

    if [ "$FORCE" = true ]; then
        echo "Starting ScyllaDB container..."
        docker run -d \
            --name "$CONTAINER_NAME" \
            -p "$SCYLLA_PORT":9042 \
            -v "$DATA_DIR":/var/lib/scylla \
            scylladb/scylla:5.2.0
    fi
fi

# Wait for health
echo "Waiting for ScyllaDB to be ready at $HOST:$SCYLLA_PORT ..."
RETRIES=45
COUNT=0

if [ "$SKIP_DOCKER" = false ]; then
    while ! docker exec "$CONTAINER_NAME" nodetool status 2>/dev/null | grep -q "^UN"; do
        if [ $COUNT -ge $RETRIES ]; then
            echo "Error: ScyllaDB failed to respond within 90 seconds."
            docker logs "$CONTAINER_NAME" | tail -n 20
            exit 1
        fi
        sleep 2
        echo -n "."
        COUNT=$((COUNT+1))
    done
else
    while ! nc -z "$HOST" "$SCYLLA_PORT" >/dev/null 2>&1; do
        if [ $COUNT -ge $RETRIES ]; then
            echo "Error: ScyllaDB failed to respond at $HOST:$SCYLLA_PORT within 90 seconds."
            exit 1
        fi
        sleep 2
        echo -n "."
        COUNT=$((COUNT+1))
    done
fi

echo " Ready!"
echo "=== Setup Complete ==="
