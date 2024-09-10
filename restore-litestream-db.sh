#!/bin/sh

# Define the path to the database file
DB_PATH="/data/db/sync-replica.db"

# Check if the database file exists
if [ ! -f "$DB_PATH" ]; then
  echo "Database file does not exist. Restoring from backup..."
  litestream restore -if-db-not-exists -o "$DB_PATH" "$REPLICA_URL"
else
  echo "Database file already exists. Skipping restore."
fi