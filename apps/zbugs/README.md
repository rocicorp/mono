# Welcome

This is the source code for [zbugs](bugs.rocicorp.dev).

We deploy this continuously (on trunk) to aws and is our dogfood of Zero.

## Requirements

- Docker
- Node 20+

## Setup

```bash
npm install
```

### Run the "upstream" Postgres database

```bash
cd docker
docker compose up
```

### Run the zero-cache server

Create a `.env` file in the `zbugs` directory:

```ini
#### zero.config.js Variables ####

# The "upstream" authoritative postgres database
# In the future we will support other types of upstreams besides PG
ZERO_UPSTREAM_DB = "postgresql://user:password@127.0.0.1:6434/postgres"

# A separate Postgres database we use to store CVRs. CVRs (client view records)
# keep track of which clients have which data. This is how we know what diff to
# send on reconnect. It can be same database as above, but it makes most sense
# for it to be a separate "database" in the same postgres "cluster".
ZERO_CVR_DB = "postgresql://user:password@127.0.0.1:6435/postgres"

# Yet another Postgres database which we used to store a replication log.
ZERO_CHANGE_DB = "postgresql://user:password@127.0.0.1:6435/postgres"

# Place to store the SQLite data zero-cache maintains. This can be lost, but if
# it is, zero-cache will have to re-replicate next time it starts up.
ZERO_REPLICA_FILE = "/tmp/zbugs-sync-replica.db"

ZERO_LOG_LEVEL = "info"

# Use "json" for logs consumed by structured logging services.
ZERO_LOG_FORMAT = "text"

# Secret used to sign and verify the JWT
# Set this to something real if you intend to deploy
# the app.
ZERO_AUTH_SECRET = "my-localhost-testing-secret"

#### ZBugs API Server Variables ####

# The client id for the GitHub OAuth app responisble for OAuth:
# https://docs.github.com/en/apps/creating-github-apps
# Rocicorp team, see:
# https://docs.google.com/document/d/1aGHaB0L15SY67wkXQMsST80uHh4-IooTUVzKcUlzjdk/edit#bookmark=id.bb6lqbetv2lm
GITHUB_CLIENT_ID = ""
# The secret for the client
GITHUB_CLIENT_SECRET = ""


#### Vite Variables ####
VITE_PUBLIC_SERVER="http://localhost:4848"
```

Then start the server:

```bash
npm run zero
```

### Run the web app

In still another tab:

```bash
npm run dev
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
