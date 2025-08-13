# zero-cache

## Configuration

### Environment Variables

Zero-cache can be configured using environment variables:

#### Publications

- `ZERO_APP_PUBLICATIONS` - PostgreSQL publications that define tables to replicate
  - Format: Comma-separated list of publication names
  - Example: `"publication1,publication2"`
  - Default: Creates a publication for all tables in the public schema

#### Ignored Tables

- `ZERO_APP_IGNORED_PUBLICATION_TABLES` - Tables to exclude from replication
  - Format: JSON array of fully qualified table names
  - Example: `["public.audit_logs", "staging.temp_data", "analytics.raw_events"]`
  - Default: `[]` (no tables ignored)
  
  **Important notes:**
  - Table names MUST be fully qualified with schema prefix (e.g., `public.users`, not `users`)
  - Ignored tables will be created in SQLite but remain empty
  - All changes to ignored tables are dropped during replication
  - Changing this list triggers a full resync, similar to changing publications
  
  **Use cases:**
  - Exclude high-volume audit/log tables that aren't needed client-side
  - Skip temporary or staging tables
  - Ignore analytics tables with sensitive data
  - Reduce SQLite database size and sync time

## Testing

These require Docker, and are run with [Testcontainers](https://testcontainers.com/modules/postgresql/).

```bash
npm run test
```

### Coverage

To view test coverage in the VSCode editor:

- Install the [Coverage Gutters](https://marketplace.visualstudio.com/items?itemName=ryanluker.vscode-coverage-gutters) extension
- Enable Coverage Gutters Watch: `Command-Shift-8`
- Run `npm run test` to update coverage.
