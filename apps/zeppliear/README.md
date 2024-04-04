# Repliear

A high-performance issue tracker in the style of [Linear](https://linear.app/).

Built with [Replicache](https://replicache.dev), [Next.js](https://nextjs.org/),
[Pusher](https://pusher.com/), and [Postgres](https://www.postgresql.org/).

Running at [repliear.herokuapp.com](https://repliear.herokuapp.com/).

# Prerequisites

1. [Get a Replicache license key](https://doc.replicache.dev/licensing)
2. Install PostgreSQL. On MacOS, we recommend using [Postgres.app](https://postgresapp.com/). For other OSes and options, see [Postgres Downloads](https://www.postgresql.org/download/).
3. [Sign up for a free pusher.com account](https://pusher.com/) and create a new "channels" app.

# To run locally

Get the Pusher environment variables from the ["App Keys" section](https://i.imgur.com/7DNmTKZ.png) of the Pusher App UI.

**Note:** These instructions assume you installed PostgreSQL via Postgres.app on MacOS. If you installed some other way, or configured PostgreSQL specially, you may additionally need to set the `PGUSER` and `PGPASSWORD` environment variables.</p>

```
export PGDATABASE="repliear"
export NEXT_PUBLIC_REPLICACHE_LICENSE_KEY="<your license key>"
export NEXT_PUBLIC_PUSHER_APP_ID=<appid>
export NEXT_PUBLIC_PUSHER_KEY=<pusherkey>
export NEXT_PUBLIC_PUSHER_SECRET=<pushersecret>
export NEXT_PUBLIC_PUSHER_CLUSTER=<pushercluster>

# Create a new database for Repliear
psql -d postgres -c 'create database repliear'

npm install
npm run dev
```

## Credits

We started this project by forking [linear_clone](https://github.com/tuan3w/linearapp_clone). This enabled us to get the visual styling right much faster than we otherwise could have.
