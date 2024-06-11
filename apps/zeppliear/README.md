# Zeppliear

A test-bed app for Zero based on Repliear a Replicache based high-performance issue tracker in the style of [Linear](https://linear.app/).

Built with Zero and [Vite](https://vitejs.dev/),).

Running at [zeppliear.vercel.app](https://zeppliear.vercel.app/).

# To run fastify replicator and sync-runner locally

update .env URIs to point to your internal ip address (169._._._ or 192._._._) do not use the 127.0.0.1 address

```
UPSTREAM_URI = "postgresql://user:password@add.your.host.ip:6432/postgres"
SYNC_REPLICA_URI = "postgres://user:password@add.your.host.ip:6433/postgres"
REPLICATOR_HOST="127.0.0.1:3001"
```

Open two windows one with docker-compose and the other workers:

```
cd docker && docker-compose up
npm run start-workers
```

# To run web locally

```
npm install
VITE_PUBLIC_SERVER="http://127.0.0.1:3000" npm run dev
```

After you have visted the local website and the sync / replica tables have populated.

## Create the indexes on the Replica

```
./create-indexes.sh
```

# To reset clear local postgres dbs and docker volumes

```
docker-compose down
docker volume rm -f docker_pgdata_sync
docker volume rm -f docker_pgdata_upstream
```

###

## Credits

We started this project by forking [linear_clone](https://github.com/tuan3w/linearapp_clone). This enabled us to get the visual styling right much faster than we otherwise could have.
