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
node --no-warnings --loader ts-node/esm src/index.ts
```

### Debug

Easiest is to open a `Debug: JavaScript Debug Terminal` in VSCode and just set break points etc.

### reflect login

Make sure to start `apps/reflect.net` since we use reflect.net to redirect back to the temporary server the CLI starts.

```bash
cd apps/reflect.net
npm run dev-next
```

Then set the `AUTH_URL` environment variable to the server you started.

```bash
AUTH_URL=http://localhost:3000/auth node --no-warnings --loader ts-node/esm src/index.ts login
```
