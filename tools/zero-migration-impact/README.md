# zero-migration-impact

Static preflight checks for PostgreSQL migration SQL that may affect Zero.

```bash
pnpm --filter zero-migration-impact run analyze apps/zbugs/db/migrations/0007_red_omega_sentinel.sql
pnpm --filter zero-migration-impact run analyze -- --zero-version 0.25.0 apps/zbugs/db/migrations
pnpm --filter zero-migration-impact run analyze -- --json apps/zbugs/db/migrations
```

The tool reads `.sql` files or directories and reports:

- whether the migration is likely to trigger a Zero backfill
- whether it may cause `SchemaVersionNotSupported`
- whether it can spike replication lag
- whether it looks safe from Zero's point of view
- remediation steps for each issue

This is intentionally conservative. SQL files alone cannot prove which tables
are in Zero's publication or which columns are in the current client schema, so
some findings are reported as "possible" and include the assumption that the
affected table or column is published and used by active clients.

Useful options:

```bash
pnpm --filter zero-migration-impact run analyze -- --fail-on warning db/migrations
pnpm --filter zero-migration-impact run analyze -- --zero-version 0.26.0-canary.7 db/migrations
pnpm --filter zero-migration-impact run analyze -- --json db/migrations
```

`--zero-version` selects a versioned rule profile. The tool keeps these rules in
source rather than checking out old code at runtime:

- before `0.0.202410040736`: `ADD COLUMN ... DEFAULT ...` is flagged as unsafe
  because this predates restored column-default handling
- `0.0.202410040736` through before `0.26.0-canary.7`: simple defaults are
  allowed, but unsupported defaults are flagged as unsafe because Zero did not
  auto-backfill them yet
- `0.26.0-canary.7` and newer: unsupported defaults are reported as Zero
  backfills; simple constants such as `DEFAULT true`, `DEFAULT 0`, and quoted
  strings are safe
