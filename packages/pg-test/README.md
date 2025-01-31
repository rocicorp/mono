Common code to create postgres instances to run integration tests against. Currently used by `zero-cache` and `z2s`.

Usage:
In your project that needs to test against a Postgres DB:

1. Create a `vitest.config.ts` with the following contents:

```ts
import config from '../pg-test/vitest.config.ts';
export default config;
```

2. Create a `vitest.workspace.ts` with the following contents:

```ts
import config from '../pg-test/vitest.workspace.ts';
export default config;
```
