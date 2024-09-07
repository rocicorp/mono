# Welcome

If you are seeing this, you are one of the very first people to see Zero outside of Rocicorp. That must mean we think a lot of you!

## ⚠️ Warning

This is still early. There are still **many** bugs. Basically you can run this dogfood app, get a feel for what Zero will be like, and tinker with some queries. You won't be able to write your own app. But we still think it's pretty encouraging in its fledgling form.

## Setup

We do not yet have any npm packages – Zero is under rapid development and we're building it side-by-side with this demo app. The best way to play with Zero is to just play with the demo app.

From root of monorepo:

```bash
npm install
```

### Run the "upstream" Postgres database

```bash
cd apps/zeppliear/docker
docker compose up
```

This will take some time to populate the database with test data the first time.

### Run the zero-cache server

Create a `.env` file in the `zeppliear` directory:

```ini
# The "upstream" authoritative postgres database
# In the future we will support other types of upstreams besides PG
UPSTREAM_URI = "postgresql://user:password@127.0.0.1:6432/postgres"

# A separate postgres database we use to store CVRs. CVRs (client view records)
# keep track of which clients have which data. This is how we know what diff to
# send on reconnect. It can be same database as above, but it makes most sense
# for it to be a separate "database" in the same postgres "cluster".
CVR_DB_URI = "postgresql://user:password@127.0.0.1:6433/postgres"

# Uniquely identifies a single instance of the zero-cache service.
REPLICA_ID = "r1"

# Place to store the SQLite data zero-cache maintains. This can be lost, but if
# it is, zero-cache will have to re-replicate next time it starts up.
REPLICA_DB_FILE = "/tmp/sync-replica.db"

# Logging level for zero-cache service.
LOG_LEVEL = "debug"
```

Then start the server:

```bash
npm run start-zero-cache
```

This will take some time to populate the replica.

### Run the web app

In still another tab:

```bash
VITE_PUBLIC_SERVER="http://127.0.0.1:3000" npm run dev
```

After you have visited the local website and the sync / replica tables have populated.

### To clear the SQLite replica db:

```bash
rm -rf /tmp/sync-replica.db
```

### To clear the upstream postgres database

```bash
docker compose down
docker volume rm -f docker_pgdata_upstream
```

## Tour

https://www.youtube.com/watch?v=nFZ5Fz6bj_8

Please don't share.

## Known Issues

- Choosing any kind of filter (priority, status, text) often kills sync. You'll see errors in the console. You can recover by either refreshing, or if that doesnt' work clearing browser state.
- Filter by label doesn't work at all.
- Mutation propagation is slow if there are many priority or status filters selected.
