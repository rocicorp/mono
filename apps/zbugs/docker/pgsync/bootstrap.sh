#!/bin/bash

# Bootstrap script to initialize OpenSearch index and perform initial sync

echo "Waiting for services to be ready..."
sleep 10

echo "Creating OpenSearch index with mapping..."
curl -X PUT "http://opensearch:9200/issues" \
  -H 'Content-Type: application/json' \
  -d @/app/index-mapping.json

echo "Starting PGSync bootstrap (initial data load)..."
pgsync -c /app/schema.json --bootstrap

echo "Starting PGSync daemon for continuous sync..."
exec pgsync -c /app/schema.json --daemon