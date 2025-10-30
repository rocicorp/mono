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
