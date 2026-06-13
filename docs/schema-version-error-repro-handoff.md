# SchemaVersionNotSupported Repro Handoff

Branch: `codex/schema-error-repro`

## Current State

This branch captures a small repro for a `SchemaVersionNotSupported` error path where the Zero client schema includes a table whose upstream Postgres table cannot be synced because it lacks a primary key or non-null unique index.

The branch currently has one existing commit:

- `4615b4bb6 Bump Zero to 1.6.0`

The uncommitted work that this handoff commit packages is:

- `apps/zbugs/shared/schema.ts`
  - Adds `execution_results` to the zbugs Zero schema.
  - The Zero schema declares `id` as the primary key.
- `apps/zbugs/db/schema-version-not-supported-repro.sql`
  - Creates `execution_results` in Postgres without a primary key.
  - This intentionally makes the table incompatible with the Zero schema.
- `packages/zero-client/src/client/schema-version-repro.test.ts`
  - Reproduces repeated `SchemaVersionNotSupported` errors and records reload timing/backoff state.
  - The expected reload intervals are currently:
    - `1500`
    - `1500`
    - `2000`
    - `4000`
    - `8000`
    - `16000`

## How To Resume

Fetch and check out the branch:

```sh
git fetch origin codex/schema-error-repro
git checkout codex/schema-error-repro
```

Run the focused client repro test:

```sh
PATH=/Users/aa/.nvm/versions/node/v24.11.1/bin:$PATH npm --workspace=zero-client run test -- src/client/schema-version-repro.test.ts
```

Note: this branch is currently npm-workspace based. `pnpm --filter ...` refuses to run with `This project is configured to use npm`.

To exercise the zbugs database mismatch manually, apply:

```sh
psql "$DATABASE_URL" -f apps/zbugs/db/schema-version-not-supported-repro.sql
```

Then start zbugs/zero-cache using the usual zbugs workflow and connect a client whose schema includes `execution_results`.

## Investigation Notes

The repro intentionally creates a mismatch:

- Zero schema says `execution_results.id` is the primary key.
- Postgres table has `id text` but no primary key and no non-null unique index.

The expected zero-cache error message is:

```text
The "execution_results" table is missing a primary key or non-null unique index and thus cannot be synced to the client
```

The client repro test focuses on the reload loop behavior after that error, not on fixing the schema mismatch itself.

## Verification Status

Attempted on 2026-06-12:

```sh
PATH=/Users/aa/.nvm/versions/node/v24.11.1/bin:$PATH npm --workspace=zero-client run test -- src/client/schema-version-repro.test.ts
```

The run did not reach the test body because the local Playwright browser binary was missing:

```text
browserType.launch: Executable doesn't exist at /Users/aa/Library/Caches/ms-playwright/chromium_headless_shell-1223/chrome-headless-shell-mac-arm64/chrome-headless-shell
```

The suggested environment fix from Playwright was `npx playwright install`.

## Next Questions

- Should `SchemaVersionNotSupported` from a persistent schema mismatch eventually stop reloading and surface a stable error state?
- Should reload backoff include page-load-to-error time, as this repro currently observes?
- Should zbugs keep this table in the schema as a repro fixture, or should the final fix move this into a narrower unit/integration test fixture?
