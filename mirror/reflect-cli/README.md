# reflect-cli

creates the console command handler for rocicorp reflect

## How to use

This is the intended usage once things are done.

```bash
npx reflect command args
```

## Developer Mode

You can use ts-node to run and debug the reflect-cli

```bash
npm run start command [args]
```

### Debug

Easiest is to open a `Debug: JavaScript Debug Terminal` in VSCode and just set
break points etc.

## Current Usage

Things are still a bit in flux but here is a typical usage.

To login we need to redirect from a web server. Eventually, this will work from
the deployed reflect.net server but for now we need to start a the server
locally.

```bash
cd apps/reflect.net
npm run dev-next
```

## `npm run start` vs `npm run start-local`

`run start uses `--stack staging` which means that it uses the deployed staging
version for the firebase functions.

`run start-local` uses `--stack local` which means that it uses the local
firebase emulator.

To run the local firebase emulator:

```bash
cd mirror/reflect-server
npm run start
```

You can also do `npm run build:watch` in the same directory if you are actively
working on the firebase functions.

## reflect init

Init selects the the current app to work on and creates a `reflect.config.js`
file in the current project directory.

```bash
npm run start init
#or
npm run start-local init
```

The difference between `start` and `start-local` is that `start-local` uses
`--stack local` which means that it uses the local firebase emulator. `start`
uses the deployed staging versions of the firebase functions.

If you have multiple applications you will see something like:

```
User is member of team(s) with multiple apps:

  veiled-encouraging-opal-xyz1 (appID: xyz1, channel: stable)
  lavish-medieval-meat-xyz2 (appID: xyz2, channel: stable)

Please specify which app to use with --name flag.
```

```
npm run start-local init --name veiled-encouraging-opal-xyz1
```

You can also create a new app (if your account allows more apps) with:

```
npm run start-local init --new
```

This is all pretty much WIP. We want to allow user provided names and renames so
this will all change.

## reflect login

We login as needed so there should be no need to run this command directly.

Make sure to start `apps/reflect.net` since we use reflect.net to redirect back
to the temporary server the CLI starts.

```bash
cd apps/reflect.net
npm run dev-next
```

Then set the `AUTH_URL` environment variable to the server you started.

```bash
AUTH_URL=http://localhost:3000/auth npm run start login
```

## reflect publish

```bash
npm run start-local publish example/index.ts
```

This will bundle/compile the `example/index.ts` file, then send it to the
mirror-server which in turn publishes to Cloudflare.
