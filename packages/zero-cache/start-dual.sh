#!/bin/sh

# Check if the database file exists
if [ ! -f "$ZERO_REPLICA_FILE" ]; then
  echo "Database file does not exist. Restoring from backup..."
  litestream restore -if-db-not-exists -if-replica-exists -o "$ZERO_REPLICA_FILE" "$REPLICA_URL"
else
  echo "Database file already exists. Skipping restore."
fi

if [ "$ZERO_LITESTREAM" = "1" ] || [ "$ZERO_LITESTREAM" = "true" ]; then
  echo "Launching Litestream"
  litestream replicate -config /opt/app/prod/zbugs/litestream.yml
else 
  echo "Not Launching Litestream and running main"
  npx tsx ./src/server/multi/main.ts
fi



