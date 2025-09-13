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
# In apps/zbugs
npm run db-up
npm run db-migrate
npm run db-seed
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
# In apps/zbugs/docker
docker compose down -v
```
