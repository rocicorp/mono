OpenSearch service (port 9200):

- Configured with 512MB heap memory for development
- Security plugin disabled for easier local development
- Single-node cluster configuration
- Health check included

OpenSearch Dashboards (port 5601):

- Web UI for visualizing and managing OpenSearch
- Security disabled to match OpenSearch config
- Depends on OpenSearch service being healthy

To start everything, run from the docker directory:
docker compose up

Services will be available at:

- PostgreSQL Primary: localhost:6434
- PostgreSQL Replica: localhost:6435
- OpenSearch: localhost:9200
- OpenSearch Dashboards: localhost:5601

---

- # PostgreSQL to OpenSearch Synchronization
       2 +
       3 +  ## Overview
       4 +  This document outlines the strategy for keeping OpenSearch synchronized with PostgreSQL data for the bug
         +  tracker application.
       5 +
       6 +  ## Synchronization Strategies
       7 +
       8 +  ### 1. Initial Data Loading
       9 +  - **Script**: `initial-load.js` - Bulk loads existing PostgreSQL data into OpenSearch
      10 +  - **When to use**: First setup, complete reindex, or data recovery
      11 +
      12 +  ### 2. Real-time Synchronization Options
      13 +
      14 +  #### Option A: Debezium + Kafka Connect (Production-grade)
      15 +  - Uses PostgreSQL's logical replication to capture changes
      16 +  - Debezium reads the WAL (Write-Ahead Log)
      17 +  - Kafka Connect OpenSearch Sink pushes changes to OpenSearch
      18 +  - **Pros**: Reliable, scalable, handles failures gracefully
      19 +  - **Cons**: More complex setup, additional infrastructure
      20 +
      21 +  #### Option B: PostgreSQL Triggers + LISTEN/NOTIFY (Simpler approach)
      22 +  - Database triggers on INSERT/UPDATE/DELETE
      23 +  - Node.js service listens for NOTIFY events
      24 +  - Updates OpenSearch in near real-time
      25 +  - **Pros**: Simpler setup, fewer moving parts
      26 +  - **Cons**: Requires application code, potential for message loss
      27 +
      28 +  #### Option C: Application-level Dual Writes
      29 +  - Application writes to both PostgreSQL and OpenSearch
      30 +  - **Pros**: Simple, full control
      31 +  - **Cons**: Consistency challenges, code duplication
      32 +
      33 +  ## Recommended Approach for Development
      34 +
      35 +  For this bug tracker, we'll implement **Option B** (PostgreSQL Triggers + LISTEN/NOTIFY) as it provides
         + a good balance of simplicity and reliability for development/small-scale production.
      36 +
      37 +  ## Data to Index
      38 +
      39 +  Based on the schema, we'll index:
      40 +  - **Issues**: title, description, status, creator, assignee, labels
      41 +  - **Comments**: body, creator, issue reference
      42 +  - **Users**: login, name (for autocomplete)
      43 +
      44 +  ## Search Features Enabled
      45 +  - Full-text search on issue titles and descriptions
      46 +  - Comment search
      47 +  - Filter by status, assignee, creator, labels
      48 +  - Autocomplete for users and labels

Perfect! I've successfully set up PGSync with OpenSearch for your bug tracker. Here's what was added:

Components Added:

1. Redis (port 6379): For change tracking and queuing
2. PGSync: Syncs PostgreSQL data to OpenSearch in real-time
3. Configuration files:


    - pgsync/schema.json: Defines the data relationships
    - pgsync/index-mapping.json: OpenSearch index structure
    - share_sql/pgsync_setup.sql: PostgreSQL publication setup
    - pgsync/bootstrap.sh: Initialization script

How It Works:

1. Initial Load: PGSync bootstraps all existing data to OpenSearch
2. Real-time Sync: Uses PostgreSQL logical replication to capture changes
3. Denormalization: Automatically nests comments within issues and resolves user names and labels

To Start Everything:

cd docker
docker compose up --build

Services Available:

- PostgreSQL: localhost:6434
- OpenSearch: localhost:9200
- OpenSearch Dashboards: localhost:5601
- Redis: localhost:6379

Search Query Example:

curl -X GET "localhost:9200/issues/\_search" \
 -H 'Content-Type: application/json' \
 -d '{
"query": {
"bool": {
"must": [
{
"multi_match": {
"query": "search term",
"fields": ["title", "description", "comment.body"]
}
}
],
"filter": [
{"term": {"assigneeName": "john"}},
{"terms": {"labels": ["bug", "urgent"]}}
]
}
},
"sort": [{"created": "desc"}]
}'

PGSync handles all the complexity of keeping the denormalized data in sync when issues, comments, users, or labels
change.
