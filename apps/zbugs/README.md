# Welcome

This is the source code for [zbugs](https://bugs.rocicorp.dev/).

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

# Where to send custom mutations
ZERO_PUSH_URL = "http://localhost:5173/api/push"

# Place to store the SQLite data zero-cache maintains. This can be lost, but if
# it is, zero-cache will have to re-replicate next time it starts up.
ZERO_REPLICA_FILE = "/tmp/zbugs-sync-replica.db"

ZERO_LOG_LEVEL = "info"

# Use "json" for logs consumed by structured logging services.
ZERO_LOG_FORMAT = "text"

# Public key used to verify JWTs.
# You can create a JWK pair via `npm run create-keys`
# in the `zbugs` directory.
#
# The public key goes here and in `VITE_PUBLIC_JWK`.
# The private key goes in `PRIVATE_JWK`
ZERO_AUTH_JWK=''

#### ZBugs API Server Variables ####

# The client id for the GitHub OAuth app responisble for OAuth:
# https://docs.github.com/en/apps/creating-github-apps
# Rocicorp team, see:
# https://docs.google.com/document/d/1aGHaB0L15SY67wkXQMsST80uHh4-IooTUVzKcUlzjdk/edit#bookmark=id.bb6lqbetv2lm
GITHUB_CLIENT_ID = ""
# The secret for the client
GITHUB_CLIENT_SECRET = ""
# See comment on `ZERO_AUTH_JWK`
PRIVATE_JWK = ""


#### Vite Variables ####
VITE_PUBLIC_SERVER="http://localhost:4848"
# See comment on `ZERO_AUTH_JWK`
VITE_PUBLIC_JWK=''

# Discord webhook to send notifications to. Not required. Notifications won't
# be sent if absent.
DISCORD_WEBHOOK_URL=''
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
docker compose down -v
```

---

## To Run 1.5GB Rocinante Data

```bash
cd docker
docker compose down -v
```

Pull large data set from s3

```bash
./get-data.sh
```

Start docker 1gb compose file

```bash
docker compose -f ./docker-compose-1gb.yml up
```

Modify the front end so that it doesn't load all of the data

```
diff --git a/apps/zbugs/src/pages/list/list-page.tsx b/apps/zbugs/src/pages/list/list-page.tsx
index 33cf7ef0b..c6955f753 100644
--- a/apps/zbugs/src/pages/list/list-page.tsx
+++ b/apps/zbugs/src/pages/list/list-page.tsx
@@ -93,6 +93,8 @@ export function ListPage({onReady}: {onReady: () => void}) {
     q = q.whereExists('labels', q => q.where('name', label));
   }

+  q = q.limit(200);
+
   const [issues, issuesResult] = useQuery(q);
   if (issues.length > 0 || issuesResult.type === 'complete') {
     onReady();
diff --git a/apps/zbugs/src/zero-setup.ts b/apps/zbugs/src/zero-setup.ts
index 020330c40..8d0223a6a 100644
--- a/apps/zbugs/src/zero-setup.ts
+++ b/apps/zbugs/src/zero-setup.ts
@@ -60,7 +60,9 @@ export function preload(z: Zero<Schema>) {

   const baseIssueQuery = z.query.issue
     .related('labels')
-    .related('viewState', q => q.where('userID', z.userID));
+    .related('viewState', q => q.where('userID', z.userID))
+    .orderBy('modified', 'desc')
+    .limit(200);

   const {cleanup, complete} = baseIssueQuery.preload();
   complete.then(() => {
```

Start zero and the frontend like normal
