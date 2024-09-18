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
brew install supabase/tap/supabase
```

### Run the "upstream" Postgres database

```bash
cd apps/zbugs
supabase start
```

### Run the zero-cache server

Create a `.env` file in the `zbugs` directory:

```ini
# Uniquely identifies a single instance of the zero-cache service.
REPLICA_ID = "r1"

# Place to store the SQLite data zero-cache maintains. This can be lost, but if
# it is, zero-cache will have to re-replicate next time it starts up.
REPLICA_DB_FILE = "/tmp/zbugs-sync-replica.db"

# Logging level for zero-cache service.
LOG_LEVEL = "debug"

# If you want to enable github auth, create a github app and set these.
# Otherwise it can be used with vanilla email auth or no auth.
SUPABASE_AUTH_GITHUB_CLIENT_ID = ""
SUPABASE_AUTH_GITHUB_SECRET = ""
```

Then start the server:

```bash
npm run zero
```

### Run the web app

From the `zbugs` directory, in another tab:

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
supabase db reset
```
