# Security Audit Report: packages/zero-server/

## Executive Summary

This security audit identified **1 high-severity**, **2 medium-severity**, and **3 low-severity** security concerns in the `zero-server` package. The codebase demonstrates strong SQL injection prevention through parameterized queries, but has gaps in input validation for the schema parameter and information disclosure through error handling.

---

## HIGH SEVERITY FINDINGS

### 1. Schema Parameter Insufficient Validation

**Location**:
- `packages/zero-protocol/src/push.ts:222-225`
- `packages/zero-server/src/zql-database.ts:60-65, 78, 93`

**Issue**: The `schema` parameter (specifying the PostgreSQL schema name) is validated only as a string with no restrictions:

```typescript
// packages/zero-protocol/src/push.ts:222-225
export const pushParamsSchema = v.object({
  schema: v.string(),  // ← No pattern/length/allowlist validation
  appID: v.string(),
});
```

This parameter is then used directly in SQL queries with `sql.ident()`:

```typescript
// packages/zero-server/src/zql-database.ts:60
sql`INSERT INTO ${sql.ident(upstreamSchema)}.clients ...`
```

**Risk**: While `sql.ident()` provides identifier escaping (preventing SQL injection), an attacker could:
1. **Schema enumeration**: Probe for other schema names in the database by observing error responses
2. **Information leakage**: Error messages may reveal existence of other schemas
3. **Denial of service**: Extremely long schema names could cause performance issues

**Recommendation**: Add validation to restrict schema names to an allowlist or pattern:
```typescript
schema: v.string().assert(s => /^[a-z_][a-z0-9_]{0,62}$/i.test(s), 'Invalid schema name')
```

---

## MEDIUM SEVERITY FINDINGS

### 2. Error Information Disclosure to Clients

**Location**:
- `packages/shared/src/error.ts:56-82`
- `packages/zero-server/src/process-mutations.ts:168-179, 197-208, 218-229, 329-349`
- `packages/zero-server/src/queries/process-queries.ts:116-132, 144-154, 174-188`

**Issue**: The `getErrorDetails()` function exposes error type names to clients:

```typescript
// packages/shared/src/error.ts:64-66
if (error.name && error.name !== 'Error') {
  return {name: error.name};  // Exposes "SyntaxError", "TypeError", etc.
}
```

Error responses include these details:

```typescript
// packages/zero-server/src/process-mutations.ts:171-178
const message = `Failed to parse push body: ${getErrorMessage(error)}`;
const details = getErrorDetails(error);
return {
  ...
  message,
  ...(details ? {details} : {}),  // Error details sent to client
};
```

**Risk**:
- Error names reveal implementation details (framework, library versions)
- Could assist attackers in crafting targeted exploits
- Stack traces or internal paths could leak via error causes

**Recommendation**:
- Sanitize error details before client response
- Only expose explicitly-set `details` from `ApplicationError`, not auto-extracted error names

---

### 3. Debug Logging of Mutation Arguments

**Location**: `packages/zero-server/src/process-mutations.ts:278-280`

**Issue**: Mutation arguments are logged at debug level:

```typescript
lc.debug?.(
  `Processing mutation '${m.name}' (id=${m.id}, clientID=${m.clientID})`,
  m.args,  // ← Full mutation arguments logged
);
```

**Risk**: If debug logging is enabled in production:
- User PII could be written to logs
- Passwords, tokens, or sensitive data in mutation args would be exposed
- Logs could become a compliance liability (GDPR, HIPAA)

**Recommendation**:
- Remove argument logging or redact sensitive fields
- Add configuration to explicitly opt-in to argument logging
- Document that debug logging should never be enabled in production

---

## LOW SEVERITY FINDINGS

### 4. Passthrough Mode in Query Parameter Parsing

**Location**: `packages/zero-server/src/process-mutations.ts:217`

**Issue**: Query parameters are parsed with `'passthrough'` mode:

```typescript
queryParams = v.parse(queryStringObj, pushParamsSchema, 'passthrough');
```

**Risk**: Extra parameters are silently accepted, which could:
- Enable parameter pollution attacks
- Allow injection of unexpected data that survives to application code
- Mask misconfigured clients

**Recommendation**: Use strict parsing mode to reject unexpected parameters.

---

### 5. Silent Cleanup Error Handling

**Location**: `packages/zero-server/src/process-mutations.ts:604-609`

**Issue**: Invalid cleanup mutations are silently ignored:

```typescript
const parseResult = v.test(mutation.args[0], cleanupResultsArgSchema);
if (!parseResult.ok) {
  lc.warn?.('Cleanup mutation has invalid args', parseResult.error);
  return;  // Silently continues
}
```

**Risk**:
- Could mask attack attempts or malformed data
- Makes debugging difficult
- Inconsistent with other mutation error handling

**Recommendation**: Return error response consistent with other mutations or track failed cleanups for monitoring.

---

### 6. Full Error Objects in Server Logs

**Location**: Multiple locations in `process-mutations.ts` and `process-queries.ts`

**Issue**: Full error objects passed to logger:

```typescript
lc.error?.('Failed to parse push body', error);  // Full error object
```

**Risk**:
- Stack traces in logs could leak sensitive paths
- Error causes may contain sensitive context
- Log aggregation systems might index sensitive data

**Recommendation**: Use structured logging with explicit fields rather than passing full error objects.

---

## SECURITY STRENGTHS

The codebase demonstrates several security best practices:

| Area | Implementation | Status |
|------|---------------|--------|
| **SQL Injection** | Parameterized queries with `sql` template tag | ✅ Properly mitigated |
| **Identifier Escaping** | Uses `@databases/escape-identifier` | ✅ Properly implemented |
| **Input Validation** | Valita schema validation on all requests | ✅ Good coverage |
| **Command Execution** | No `eval()`, `exec()`, `spawn()` found | ✅ No risk |
| **File System** | No file operations found | ✅ No path traversal risk |
| **Mutation Ordering** | Deduplication and ordering enforcement | ✅ Prevents replay attacks |

---

## ARCHITECTURAL SECURITY CONSIDERATIONS

### Authentication/Authorization
Zero-server **intentionally** does not implement authentication or authorization. Applications must:
1. Validate authentication tokens before calling `processor.process()`
2. Pass user context via the generic context parameter
3. Implement authorization checks within mutator handlers

**This is documented but warrants prominent security warnings.**

### Row-Level Security
No RLS enforcement at the zero-server layer. Applications should:
- Use PostgreSQL RLS policies as defense-in-depth
- Implement application-level access checks in mutators

---

## RECOMMENDED REMEDIATIONS

| Priority | Finding | Remediation |
|----------|---------|-------------|
| **P1** | Schema parameter validation | Add pattern/allowlist validation |
| **P2** | Error details leakage | Remove `error.name` from client responses |
| **P2** | Debug logging of args | Redact or remove argument logging |
| **P3** | Passthrough parsing | Switch to strict validation mode |
| **P3** | Silent cleanup errors | Return consistent error responses |
| **P3** | Full error objects in logs | Use structured logging |

---

## FILES REVIEWED

- `packages/zero-server/src/process-mutations.ts` (641 lines)
- `packages/zero-server/src/queries/process-queries.ts` (203 lines)
- `packages/zero-server/src/zql-database.ts` (119 lines)
- `packages/zero-server/src/custom.ts` (486 lines)
- `packages/zero-server/src/schema.ts`
- `packages/zero-server/src/pg-query-executor.ts`
- `packages/zero-server/src/adapters/pg.ts` (133 lines)
- `packages/zero-server/src/adapters/prisma.ts`
- `packages/zero-server/src/adapters/drizzle.ts`
- `packages/zero-server/src/adapters/postgresjs.ts`
- `packages/zero-protocol/src/push.ts`
- `packages/shared/src/error.ts`
- `packages/z2s/src/sql.ts`
