# Welcome

This is the source code for [zbugs](https://bugs.rocicorp.dev/).

We deploy this continuously (on trunk) to aws and is our dogfood of Zero.

## Requirements

- Docker
- Node 20+

## Setup

### 1. Install dependencies

First, install and build dependencies the `mono` repository root:

```bash
# In repository root
npm install
npm run build
```

Then, install dependencies in the `zbugs` directory:

```bash
# In apps/zbugs
npm install
```

### 2. Run the "upstream" Postgres database

```bash
cd docker
docker compose up
```

### 3. Run the zero-cache server

> In a a new terminal window

Create a `.env` file in the `zbugs` directory based on the example:

```bash
# In apps/zbugs
cp .env.example .env
```

Then start the server:

```bash
# In apps/zbugs
npm run zero-cache-dev
```

### 4. Run the web app

> In yet another another terminal window

```bash
# In apps/zbugs
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
