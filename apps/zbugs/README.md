# Welcome

If you are seeing this, you are one of the very first people to see Zero outside of Rocicorp. That must mean we think a lot of you!

## ⚠️ Warning

This is still early. There are still **many** bugs. Basically you can run this dogfood app, get a feel for what Zero will be like, and tinker with some queries. You won't be able to write your own app. But we still think it's pretty encouraging in its fledgling form.

## Setup

We do not yet have any npm packages – Zero is under rapid development and we're building it side-by-side with this demo app. The best way to play with Zero is to just play with the demo app.

First, you will need [Docker](https://docs.docker.com/engine/install/).

Then, from root of monorepo:

```bash
npm install
```

### Run the "upstream" Postgres database

```bash
cd apps/zbugs/docker
docker compose up
```

### Run the zero-cache server

Create a `.env` file in the `zbugs` directory:

```ini
#### zero.config.js Variables ####

# The "upstream" authoritative postgres database
# In the future we will support other types of upstreams besides PG
UPSTREAM_URI = "postgresql://user:password@127.0.0.1:6434/postgres"

# A separate Postgres database we use to store CVRs. CVRs (client view records)
# keep track of which clients have which data. This is how we know what diff to
# send on reconnect. It can be same database as above, but it makes most sense
# for it to be a separate "database" in the same postgres "cluster".
CVR_DB_URI = "postgresql://user:password@127.0.0.1:6435/postgres"

# Yet another Postgres database which we used to store a replication log.
CHANGE_DB_URI = "postgresql://user:password@127.0.0.1:6435/postgres"

# Uniquely identifies a single instance of the zero-cache service.
REPLICA_ID = "r1"

# Place to store the SQLite data zero-cache maintains. This can be lost, but if
# it is, zero-cache will have to re-replicate next time it starts up.
REPLICA_DB_FILE = "/tmp/zbugs-sync-replica.db"

#### ZBugs API Server Variables ####

# The client id for the GitHub app responisble for OAuth
# https://docs.github.com/en/apps/creating-github-apps
GITHUB_CLIENT_ID = ""
# The secret for the client
GITHUB_CLIENT_SECRET = ""

# Secret used to sign and verify the JWT
# Set this to something real if you intend to deploy
# the app.
JWT_SECRET = "my-localhost-testing-secret"
```

Then start the server:

```bash
npm run zero
```

### Run the web app

In still another tab:

```bash
VITE_PUBLIC_SERVER="http://[::1]:3000" npm run dev
```

After you have visited the local website and the sync / replica tables have populated.

### To clear the SQLite replica db:

```bash
rm /tmp/zbugs-sync-replica.db*
```

### To clear the upstream postgres database

```bash
docker compose down
docker volume rm -f docker_zbugs_pgdata_sync docker_zbugs_pgdata_upstream
```
