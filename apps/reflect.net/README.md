## Reflect.net

Build everything at the top level first:

`cd ../.. && npm run build && cd -`

Copy the .env files:

`cp .env.example .env; cp .dev.vars.example .dev.vars`

In separate terminals, run:

`npm run dev-worker`

`npm run dev-next`
