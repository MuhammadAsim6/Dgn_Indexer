#!/bin/bash
# ------------------------------------------------------------------------------
# 🛰️ ZIGCHAIN INDEXER - FRESH START SCRIPT
# ------------------------------------------------------------------------------
# ⚠️ This script wipes the database data and restarts the indexer.
# Used for testing migrations and applying schema changes to a fresh DB.

# 1. Stop containers and remove volumes
docker-compose down -v

# 2. Wipe the data directory (requires sudo)
DATA_DIR="/media/asim/ubuntu v2/indexer-data-v2"
echo "Targeting: $DATA_DIR"

if [ -d "$DATA_DIR" ]; then
    echo "Directory exists. Nuking..."
    sudo rm -rf "$DATA_DIR"
fi

# 3. Recreate with correct permissions for Timescale-HA (UID 1000)
echo "Recreating $DATA_DIR with correct permissions..."
sudo mkdir -p "$DATA_DIR"
sudo chown -R 1000:1000 "$DATA_DIR"

# 4. Rebuild and start
echo "Rebuilding and starting indexer..."
docker-compose up -d --build --force-recreate

echo "✅ Indexer is restarting. Use 'docker-compose logs -f' to follow."
