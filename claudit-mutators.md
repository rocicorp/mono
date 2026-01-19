# Security Audit Report: Custom Mutators Implementation

**Audit Date**: 2026-01-19
**Packages Reviewed**: `zero-server`, `zql`, `zero-cache`
**Focus Area**: Custom mutators security

---

## Executive Summary

The custom mutator system has **solid security fundamentals** with parameterized SQL queries and proper identifier escaping. However, several areas require attention for production deployments.

---

## Critical Findings

### 1. Silent Authorization Failures (Medium-High Severity)

**Location**: `packages/zero-cache/src/services/mutagen/mutagen.ts:362-363`

```typescript
// Confirm the mutation even though it may have been blocked by the authorizer.
// Authorizer blocking a mutation is not an error but the correct result of the mutation.
tasks.unshift(() =>
  checkSchemaVersionAndIncrementLastMutationID(...)
);
```

When authorization fails, the mutation is marked as "processed" but not executed. The client receives no explicit error—they see success but data is unchanged.

**Risk**: Security-sensitive applications may incorrectly assume mutations succeeded, leading to inconsistent client state or confused users.

**Recommendation**: Add configuration option to return explicit authorization failure errors to clients.

---

### 2. Missing Table Name Validation in CRUD Mutations (Medium Severity)

**Location**: `packages/zero-cache/src/services/mutagen/mutagen.ts:378-428`

```typescript
export function getInsertSQL(tx: postgres.TransactionSql, create: InsertOp) {
  return tx`INSERT INTO ${tx(create.tableName)} ${tx(create.value)}`;
}
```

Table names from client requests are passed directly to SQL execution. While the `postgres.js` library properly escapes identifiers (preventing SQL injection), there's **no validation** that:
- The table exists in the schema
- The user has permissions to access that table at the schema level

**Mitigation**: The postgres.js escaping prevents SQL injection, but invalid tables cause runtime PostgreSQL errors rather than clean validation failures.

**Recommendation**: Validate `tableName` against the Zero schema before SQL execution.

---

### 3. Authentication Context is Optional (High Severity)

**Location**: `packages/zero-cache/src/services/mutagen/mutagen.ts:51-55`

```typescript
processMutation(
  mutation: Mutation,
  authData: JWTPayload | undefined,  // Can be undefined!
  customMutatorsEnabled: boolean,
): Promise<MutationError | undefined>;
```

**Location**: `packages/zero-server/src/process-mutations.ts:139`

```typescript
cb: (
  transact: TransactFn<D>,
  mutation: CustomMutation,
) => Promise<MutationResponse>,
```

The mutation processors accept `undefined` authentication. If the application fails to attach `authData`, mutations execute without any identity context.

**Risk**: Authentication bypass if the app doesn't properly integrate auth into the request flow.

**Recommendation**:
- Document clearly that apps MUST provide `authData` for security
- Consider adding a configuration flag to require non-undefined `authData`

---

### 4. No Rate Limiting for Custom Mutators (Medium Severity)

**Location**: `packages/zero-cache/src/services/mutagen/mutagen.ts:100-105`

```typescript
if (config.perUserMutationLimit.max !== undefined) {
  this.#limiter = new SlidingWindowLimiter(
    config.perUserMutationLimit.windowMs,
    config.perUserMutationLimit.max,
  );
}
```

Rate limiting only applies to CRUD mutations in `MutagenService`. Custom mutators executed via `handleMutateRequest()` in `zero-server` have **no rate limiting**.

**Risk**: DoS via expensive custom mutator operations.

**Recommendation**: Extend rate limiting infrastructure to cover custom mutator endpoints.

---

### 5. No Execution Timeout for Mutators (Medium Severity)

**Location**: `packages/zql/src/mutate/mutator-registry.ts:285-298`

```typescript
const fn: MutatorDefinitionFunction<...> = async options => {
  const validatedArgs = validator
    ? validateInput(name, options.args, validator, 'mutator')
    : (options.args as unknown as ArgsOutput);
  await definition.fn({
    args: validatedArgs,
    ctx: options.ctx,
    tx: options.tx,
  });  // No timeout enforcement!
};
```

Mutator functions have no timeout enforcement. An infinite loop or slow I/O in a mutator blocks the transaction indefinitely.

**Risk**: Resource exhaustion, transaction lock contention, denial of service.

**Recommendation**: Add configurable timeout wrapper around mutator execution.

---

## Security Strengths

### 1. SQL Injection Prevention ✅

**Location**: `packages/z2s/src/sql.ts`

The system uses the `@databases/sql` library with proper parameterization:

```typescript
import {
  escapePostgresIdentifier,
  escapeSQLiteIdentifier,
} from '@databases/escape-identifier';
```

- Identifiers escaped via `escapePostgresIdentifier`/`escapeSQLiteIdentifier`
- Values passed as parameters, never string-interpolated
- Type-aware conversion with explicit casts

**Example from** `packages/zero-server/src/custom.ts:355-367`:

```typescript
const stmt = formatPgInternalConvert(
  sql`INSERT INTO ${sql.ident(serverName(schema))} (${sql.join(
    targetedColumns.map(([, serverName]) => sql.ident(serverName)),
    ',',
  )}) VALUES (${sql.join(
    Object.entries(value).map(([col, v]) =>
      sqlInsertValue(v, serverTableSchema[serverNameFor(col, schema)]),
    ),
    ', ',
  )})`,
);
```

---

### 2. Input Validation Framework ✅

**Location**: `packages/zql/src/query/validate-input.ts`

```typescript
export function validateInput<TInput, TOutput>(
  name: string,
  input: TInput,
  validator: StandardSchemaV1<TInput, TOutput> | undefined,
  kind: 'query' | 'mutator',
): TOutput {
  if (!validator) {
    return input as unknown as TOutput;
  }

  const result = validator['~standard'].validate(input);
  if (result instanceof Promise) {
    throw new Error(
      `Async validators are not supported. ${titleCase(kind)} name ${name}`,
    );
  }
  if (result.issues) {
    throw new Error(
      `Validation failed for ${kind} ${name}: ${result.issues
        .map(issue => issue.message)
        .join(', ')}`,
    );
  }
  return result.value;
}
```

Key security properties:
- StandardSchema spec compliance (vendor-agnostic: Zod, Valibot, etc.)
- **Synchronous validation only** — async validators are rejected, preventing timing attacks
- Validation happens before mutator function execution
- Transformed output type flows safely to mutator

---

### 3. Mutation ID Tracking ✅

**Location**: `packages/zero-cache/src/services/mutagen/mutagen.ts:431-461`

```typescript
async function checkSchemaVersionAndIncrementLastMutationID(
  tx: PostgresTransaction,
  shard: ShardID,
  clientGroupID: string,
  clientID: string,
  receivedMutationID: number,
) {
  const [{lastMutationID}] = await tx<{lastMutationID: bigint}[]>`
    INSERT INTO ${tx(upstreamSchema(shard))}.clients
      as current ("clientGroupID", "clientID", "lastMutationID")
          VALUES (${clientGroupID}, ${clientID}, ${1})
      ON CONFLICT ("clientGroupID", "clientID")
      DO UPDATE SET "lastMutationID" = current."lastMutationID" + 1
      RETURNING "lastMutationID"
  `;

  if (receivedMutationID < lastMutationID) {
    throw new MutationAlreadyProcessedError(...);
  } else if (receivedMutationID > lastMutationID) {
    throw new ProtocolError({kind: ErrorKind.InvalidPush, ...});
  }
}
```

- Prevents duplicate mutation processing
- Prevents out-of-order mutations
- Uses `SERIALIZABLE` transaction isolation

---

### 4. Two-Phase Authorization ✅

**Location**: `packages/zero-cache/src/auth/write-authorizer.ts`

| Operation | Pre-Mutation Check | Post-Mutation Check | Rationale |
|-----------|-------------------|---------------------|-----------|
| Insert    | ❌ No             | ✅ Yes              | No pre-state exists to check |
| Update    | ✅ Yes            | ✅ Yes              | Check both old and new state |
| Delete    | ✅ Yes            | ❌ No               | No post-state exists to check |

```typescript
async canPreMutation(authData: JWTPayload | undefined, ops: Exclude<CRUDOp, UpsertOp>[]) {
  for (const op of ops) {
    switch (op.op) {
      case 'insert':
        // insert does not run pre-mutation checks
        break;
      case 'update':
        if (!(await this.#canUpdate('preMutation', authData, op))) {
          return false;
        }
        break;
      case 'delete':
        if (!(await this.#canDelete('preMutation', authData, op))) {
          return false;
        }
        break;
    }
  }
  return true;
}
```

---

### 5. Protocol Validation ✅

**Location**: `packages/zero-protocol/src/push.ts`

All incoming push data is validated against Valita schemas:

```typescript
export const pushBodySchema = v.object({
  clientGroupID: v.string(),
  mutations: v.array(mutationSchema),
  pushVersion: v.number(),
  schemaVersion: v.number().optional(),
  timestamp: v.number(),
  requestID: v.string(),
});

const insertOpSchema = v.object({
  op: v.literal('insert'),
  tableName: v.string(),
  primaryKey: primaryKeySchema,
  value: rowSchema,
});
```

---

## Recommendations Summary

### Critical Priority

| Issue | Recommendation |
|-------|----------------|
| Authentication can be undefined | Document requirements; add config flag to require auth |
| Silent authorization failures | Add option to return explicit authorization errors |
| Table names not validated | Validate against Zero schema before SQL execution |

### High Priority

| Issue | Recommendation |
|-------|----------------|
| No mutator execution timeout | Add configurable timeout wrapper |
| No rate limiting for custom mutators | Extend rate limiting to `handleMutateRequest()` |
| No input size limits | Enforce max size for args arrays and nesting depth |

### Medium Priority

| Issue | Recommendation |
|-------|----------------|
| Error details may leak info | Audit `error.details` contents |
| Authorization asymmetry undocumented | Document pre/post check differences |
| Request tracing incomplete | Ensure `requestID` logged consistently |

---

## Key Security-Critical Files

| File | Security Role |
|------|---------------|
| `packages/zero-cache/src/services/mutagen/mutagen.ts` | CRUD mutation processing, rate limiting |
| `packages/zero-cache/src/auth/write-authorizer.ts` | Authorization policy enforcement |
| `packages/zero-server/src/process-mutations.ts` | Custom mutation request handling |
| `packages/zero-server/src/custom.ts` | Server-side SQL generation |
| `packages/zql/src/query/validate-input.ts` | Input validation |
| `packages/zql/src/mutate/mutator-registry.ts` | Mutator registration and lookup |
| `packages/z2s/src/sql.ts` | SQL parameterization and escaping |
| `packages/zero-protocol/src/push.ts` | Wire protocol schemas |

---

## Data Flow: Client Request to Database

```
Client Request (POST /push)
    ↓
Request Parsing & Validation
├── Push body validation (Valita schema)
├── Query parameters (schema, appID)
└── Mutation type validation (CRUD vs Custom)
    ↓
Mutation Processing Loop
├── For Custom Mutations:
│   ├── Dispatch to user handler (mutator name lookup)
│   ├── Handler receives: (tx, mutatorName, mutatorArgs)
│   ├── Input validation (if validator defined)
│   ├── Handler may call: tx.mutate.table.operation()
│   └── Custom code runs in transaction
│
└── For CRUD Mutations:
    ├── Operations parsed from args[0].ops
    ├── Authorizer.normalizeOps() converts upsert→insert/update
    ├── Pre-mutation authorization checks
    ├── Post-mutation authorization checks (via replica simulation)
    ├── Generate SQL for each op:
    │   ├── INSERT: parameterized with type conversion
    │   ├── UPDATE: primaryKeyClause + parameterized values
    │   ├── DELETE: primaryKeyClause only
    │   └── UPSERT: INSERT + ON CONFLICT DO UPDATE
    ├── Execute SQL via database adapter
    └── Increment lastMutationID
        ↓
Database Execution
├── PostgreSQL (upstream, SERIALIZABLE isolation)
├── SQLite replica (zero-cache, for auth simulation)
└── Replicache IndexedDB (client-side)
```

---

## Conclusion

The custom mutator system is **suitable for production** with proper application-level integration:

1. **Authentication must be correctly integrated** — apps must provide `authData`
2. **Silent authorization failures are by design** — document this behavior for users
3. **Operational limits needed** — timeouts and rate limiting should be enforced at the deployment level

The core security model (parameterized SQL, input validation, authorization framework) is sound.