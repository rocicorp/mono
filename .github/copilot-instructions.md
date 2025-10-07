# Rocicorp Monorepo - Copilot Instructions

## Architecture Overview

This monorepo contains **Zero** (real-time sync platform) and **Replicache** (client-side data layer), built as complementary technologies for building reactive, sync-enabled applications.

### Key Components

- **packages/zero-client**: Main Zero client library using Replicache under the hood
- **packages/zero-cache**: Server-side cache and sync engine
- **packages/zql**: IVM (Incremental View Maintenance) query engine and language
- **packages/replicache**: Core client-side data synchronization library
- **apps/zbugs**: Reference application demonstrating Zero/Replicache patterns

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

### Package-Level Commands

Each package supports: `test`, `check-types`, `lint`, `format`, `build`

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
- Tests are co-located with source files (`.test.ts`)
- Multiple vitest configs for different environments (e.g., `vitest.config.pg-16.ts` for PostgreSQL tests)
- Test files automatically discovered by the root vitest config
- Prefer `test` over `it` for consistency

### Import Patterns

- Packages import from each other using workspace names (`zero-client`, `shared`, etc.)
- Cross-package dependencies are managed through the monorepo structure
- Re-exports are common (see `packages/zero/src/zero.ts` → `packages/zero-client/src/mod.ts`)
- Do not import from `mod.ts`. Use relative paths.

## Database Integration

### Zero + PostgreSQL

Zero maintains a dual-database setup:

- **PostgreSQL**: Source of truth for server data
- **SQLite**: Client-side replica managed by Replicache

### Schema Migrations

- Use Drizzle for PostgreSQL schema management (`db-migrate`, `db-seed`)
- Zero schema definitions are separate from PostgreSQL schema
- Apps like zbugs demonstrate the connection between PostgreSQL tables and Zero schemas

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
