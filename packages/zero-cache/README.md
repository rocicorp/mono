# zero-cache

## Testing

### Postgres-agnostic Tests

These are run from within isolated `workerd` runtimes (i.e. Miniflare 3) via
Cloudflare [Workers Vitest integration](https://developers.cloudflare.com/workers/testing/vitest-integration/configuration/).

```bash
npm run test
```

### Postgres-dependent Tests

These require Docker, and are run with [Testcontainers](https://testcontainers.com/modules/postgresql/).

```bash
npm run pg-test
```

### All Tests

Runs all tests, including Postgres-dependent Tests, from within the `workerd` runtime.

This requires a local Postgres instance (e.g. https://postgres.app).

> TODO: Replace this requirement with [Testcontainers](https://testcontainers.com/modules/postgresql/).

```bash
npm run all-test
```
