# Zeppliear

A test-bed app for Zero based on Repliear a Replicache based high-performance issue tracker in the style of [Linear](https://linear.app/).

Built with Zero and [Vite](https://vitejs.dev/).

Running at [zeppliear.vercel.app](https://zeppliear.vercel.app/).

# To run fastify replicator and sync-runner locally

add .env file:

```
UPSTREAM_URI = "postgresql://user:password@127.0.0.1:6432/postgres"
CVR_DB_URI = "postgresql://user:password@127.0.0.1:6433/postgres"
REPLICA_ID = "r1"
REPLICA_DB_FILE = "/tmp/sync-replica.db"
LOG_LEVEL = "debug"
```

Open two windows one with docker-compose and the other workers:

```
cd docker && docker compose up
npm run start-zero-cache
```

# To run web locally

```
npm install
VITE_PUBLIC_SERVER="http://127.0.0.1:3000" npm run dev
```

After you have visited the local website and the sync / replica tables have populated.

# To reset clear local postgres dbs and docker volumes

```
docker compose down
docker volume rm -f docker_pgdata_upstream
```

###

## Credits

We started this project by forking [linear_clone](https://github.com/tuan3w/linearapp_clone). This enabled us to get the visual styling right much faster than we otherwise could have.
