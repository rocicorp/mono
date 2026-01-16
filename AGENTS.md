# Rocicorp Monorepo Instructions

## Architecture Overview

This monorepo contains **Zero** (real-time sync platform) and **Replicache** (client-side data layer), built as complementary technologies for building reactive, sync-enabled applications.

### Repo Structure

```
mono/
├── packages/          # 29 core packages (libraries and engines)
│   ├── zero-client    # Main Zero client (uses Replicache)
│   ├── zero-cache     # Server-side cache and sync engine
│   ├── zero-server    # Server-side mutations/queries
│   ├── zero-schema    # Schema definition builder
│   ├── zql            # IVM (Incremental View Maintenance) query engine and language
│   ├── replicache     # Core client-side sync library
│   └── shared         # Shared utilities and testing helpers
├── apps/              # 3 applications
│   ├── zbugs          # Reference app (React + Wouter + Zero + PostgreSQL)
│   ├── otel-proxy     # OpenTelemetry proxy
│   └── zql-viz        # Query visualization tool
├── tools/             # 5 development tools
└── prod/              # Production deployment (SST/Pulumi)
```

### Data Flow Architecture

Zero follows a **sync-first** model: client queries are reactive and automatically update when server data changes. ZQL queries are transformed to SQL on the server and results are incrementally maintained.

## Development Workflow

### Essential Commands

```bash
# Install and build everything
npm install && npm run build

# Run tests (uses vitest)
npm run test              # All tests
npm run test:watch        # Watch mode

# Type checking and linting
npm run check-types       # TypeScript across all packages
npm run lint              # oxlint with type-awareness
npm run format            # Prettier formatting
```

**Always run `lint`, `format` and `check-types` after every change.**

### Package-Level Commands

Prefer package-level commands when possible. Each package supports: `test`, `check-types`, `lint`, `format`, `build`. e.g.:

```bash
npm --workspace=zero-client run format
npm --workspace=zero-cache run lint
npm --workspace=zero-server run check-types

# Run with coverage (prefer using this flag when possible)
npm --workspace=zero-client run test -- --coverage

# Run specific test file
npm --workspace=zero-client run test -- zero.test
```

### Zero Cache Development

```bash
# Start Zero cache server for local development
npm run start-zero-cache

# In zbugs app - start Zero cache with schema hot-reload
npm run zero-cache-dev
```

## Code Conventions

### TypeScript Patterns

- **Optional fields**: Always explicitly typed as `type | undefined` (not just `type?`)

  ```typescript
  // Correct
  interface User {
    name?: string | undefined;
  }

  // Incorrect
  interface User {
    name?: string;
  }
  ```

### Zero Schema Definition

Zero schemas use a builder pattern with method chaining:

```typescript
const user = table('user')
  .columns({
    id: string(),
    name: string().optional(),
    role: enumeration<Role>(),
  })
  .primaryKey('id');
```

### Testing Patterns

- Use **vitest** for all testing
- Tests are co-located with source files using environment-specific naming:
  - `.test.ts` - Standard tests (Node.js environment)
  - `.node.test.ts` - Node-specific tests (Replicache)
  - `.web.test.ts` - Browser tests (Replicache)
  - `.pg.test.ts` - PostgreSQL integration tests
- Multiple vitest configs for different environments (e.g., `vitest.config.pg-16.ts` for PostgreSQL tests)
- Test files automatically discovered by the root vitest config
- Prefer `test` over `it` for consistency
- Coverage is run with `v8` - use the `--coverage` flag to help write tests

### Import Patterns

- **DO NOT import from `mod.ts`**: Use direct relative paths instead

  ```typescript
  // Correct - use relative path
  import {helper} from './helper.ts';

  // Incorrect - don't import from mod.ts
  import {helper} from './mod.ts';
  ```

- **DO NOT use `import()` in type expressions**: Always use `import type` at the top of the file

  ```typescript
  // Correct - import type at the top
  import type {AST} from '../../../zero-protocol/src/ast.ts';
  import type {TTL} from './ttl.ts';

  abstract addServerQuery(ast: AST, ttl: TTL): void;

  // Incorrect - don't use import() in type expressions
  abstract addServerQuery(
    ast: import('../../../zero-protocol/src/ast.ts').AST,
    ttl: import('./ttl.ts').TTL,
  ): void;
  ```

- **DO NOT use dynamic imports (`await import()`) unless necessary**: Use standard static imports

  ```typescript
  // Correct - static import
  import {createBuilder} from '../../../zql/src/query/named.ts';

  // Incorrect - unnecessary dynamic import
  const {createBuilder} = await import('../../../zql/src/query/named.ts');
  ```

  Dynamic imports are only needed for:
  - Lazy-loading heavy modules
  - Conditional imports based on runtime conditions

- **AVOID re-exports that create cycles**: Re-exports can introduce circular dependencies between packages

  ```typescript
  // Incorrect - re-exporting from higher-level package
  // In zero-types/src/schema.ts:
  export type {Schema} from '../zero-schema/src/builder/schema-builder.ts';

  // Correct - import directly from the source
  // In your code:
  import type {Schema} from '../zero-types/src/schema.ts';
  ```

  **Package dependency hierarchy** (lower packages should not depend on higher ones):
  - `shared`, `zero-protocol`, `zero-types` (lowest level - pure types/utilities)
  - `zql`, `zero-schema` (mid level - can use types packages)
  - `zero-client`, `zero-server`, `zero-cache` (higher level - can use zql/schema)
  - `zero` (highest - re-exports for convenience, user-facing only)

- Re-exports are acceptable in **user-facing packages** for convenience (e.g., `packages/zero/src/mod.ts` → exports from `zero-client`, `zero-server`), but avoid re-exports between internal packages

## Database

### Zero + PostgreSQL

Zero is a streaming database:

- **PostgreSQL**: Source of truth for data
- **SQLite**: Server-side replica managed by `zero-cache`
- **Replicache**: Client-side store managed by `zero-client` and `replicache`, in IndexedDB by default

### Schema Migrations

- Use Drizzle for PostgreSQL schema management (`db-migrate`, `db-seed`)
- Zero schema definitions are separate from PostgreSQL schema
- Apps like zbugs demonstrate the connection between PostgreSQL tables and Zero schemas

## Git Conventions

### Commit Messages

Follow conventional commits format:

```
type(scope): description
```

- `feat(zero-client): add support for custom mutations`
- `fix(zero-cache): resolve memory leak in connection pool`
- `chore(deps): update vitest to 3.2.4`

### Cherry-picking

Always use the `-x` flag when cherry-picking to record the source commit hash:

```bash
git cherry-pick -x <commit>
```

## Debugging and Development

### Zero Cache Debugging

```bash
# Debug Zero cache with breakpoints
npm run zero-brk

# Transform/run queries for debugging
npm run transform-query
npm run run-query
```

### Docker Development

Many apps include Docker Compose for local PostgreSQL:

```bash
npm run db-up    # Start PostgreSQL
npm run db-down  # Stop PostgreSQL
```

## Package Dependencies

### Core Dependencies

- **@rocicorp/\*** packages are internal utilities (logger, lock, resolver)
- **vitest**: Primary testing framework
- **oxlint**: TypeScript-aware linting
- **turbo**: Monorepo task running and caching

### Zero-Specific

- Clients depend on `replicache` for local data management
- Server components use `fastify` for HTTP/WebSocket handling
- OpenTelemetry integration for observability

## Critical Files to Understand

- `turbo.json`: Task dependencies and caching configuration
- `vitest.config.ts`: Multi-project test discovery and configuration
- `apps/zbugs/shared/schema.ts`: Reference Zero schema implementation
- `packages/zero-client/src/mod.ts`: Main Zero client API surface

## Running zbugs Locally

zbugs is the reference Zero application. To run it locally:

### Prerequisites

1. **Docker must be running** - Start Docker Desktop before running `db-up`

2. If you've made changes to any Zero packages (`zero-client`, `zero-cache`, `zero-protocol`, etc.), you must first rebuild:

```bash
npm --workspace=@rocicorp/zero run build
```

### Starting the Services

From `apps/zbugs`, start these three services (in background for AI, separate tabs for humans):

```bash
cd apps/zbugs

# 1. Start PostgreSQL (Docker) - must complete before others
npm run db-up

# 2. Start zero-cache with hot-reload
npm run zero-cache-dev

# 3. Start the Vite dev server
npm run dev
```

**For AI assistants**: Run `db-up` in background, wait for PostgreSQL to be ready, then run `zero-cache-dev` and `dev` in background. Use `run_in_background` parameter or `&` suffix. Check logs with `tail` on the output files.

### First-Time Setup

If the database is empty or schema has changed:

```bash
cd apps/zbugs
npm run db-migrate  # Apply schema migrations
npm run db-seed     # Seed with test data
```

### Troubleshooting

- **Port conflicts**: If `zero-cache-dev` fails with port in use, find and kill the process: `lsof -i :4848 | grep LISTEN` then `kill <PID>`
- **Schema changes**: If you modify `apps/zbugs/shared/schema.ts`, restart `zero-cache-dev`
- **Client changes**: Vite hot-reloads automatically, but for Zero client changes you may need to refresh the browser

See `apps/zbugs/README.md` for additional setup details and configuration options.
