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

## `npm run reflect` vs `npm run reflect-local`

`npm run reflect` runs the reflect cli against the prod firebase stack.

- Use `npm run reflect -- --stack=sandbox` to run against he sandbox stack.

`npm run reflect-local` will point to:

- a locally run login page (run with `apps/reflect.net$ npm run dev-next`)
- locally run cloud functions (run with `mirror/mirror-server$ npm run serve`)

You can also do `npm run build:watch` in the same directory if you are actively
working on the firebase functions.
