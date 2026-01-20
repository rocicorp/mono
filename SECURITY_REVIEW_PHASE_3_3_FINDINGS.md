# Security Review Phase 3.3 - Configuration & Secrets

## Executive Summary

This document contains the security review findings for Phase 3.3 (Configuration & Secrets):
- **5.1 Environment Variable Handling** (credential handling, secrets logging)
- **5.2 Development vs Production** (behavior differences, debug endpoints)

**Overall Assessment**: 2 medium-severity issues, 1 low-severity issue, and several positive security practices identified.

---

## Findings

### MEDIUM-01: Development Mode Authentication Bypass

**Severity**: MEDIUM
**CVSS Score**: 5.4 (Medium)
**Files**:
- `packages/zero-cache/src/config/normalize.ts:27-29`
- `packages/zero-cache/src/config/zero-config.ts:896-901`
- `packages/zero-cache/src/server/inspector-delegate.ts:111-114`

**Vulnerability Description**:

When `NODE_ENV=development`, several security controls are bypassed:

1. **Admin password requirement is waived**:
```typescript
// zero-config.ts:896-901
if (!password && !config.adminPassword && isDevelopmentMode()) {
  warnOnce(
    lc,
    'No admin password set; allowing access in development mode only',
  );
  return true;  // BYPASSES AUTH
}
```

2. **Inspector authentication is bypassed**:
```typescript
// inspector-delegate.ts:111-114
isAuthenticated(clientGroupID: ClientGroupID): boolean {
  return (
    isDevelopmentMode() || authenticatedClientGroupIDs.has(clientGroupID)
  );
}
```

**Risk Scenarios**:
- Developer accidentally deploys with `NODE_ENV=development`
- CI/CD pipeline misconfiguration sets wrong NODE_ENV
- Container environment variable override exposes production instance

**Impact**:
- Full access to `/statz` and `/heapz` endpoints without authentication
- Inspector features accessible without authentication
- Sensitive database statistics and heap snapshots can be downloaded

**Recommendation**:

1. Add explicit warning at startup when running in development mode:
```typescript
if (isDevelopmentMode()) {
  lc.warn?.('**WARNING**: Running in DEVELOPMENT mode - security controls reduced');
  lc.warn?.('Set NODE_ENV=production for production deployments');
}
```

2. Consider removing development mode bypasses entirely or adding a separate explicit flag:
```typescript
// More explicit: ZERO_ALLOW_UNAUTHENTICATED_DEBUG=true
const allowUnauthenticatedDebug =
  process.env.ZERO_ALLOW_UNAUTHENTICATED_DEBUG === 'true' &&
  isDevelopmentMode();
```

3. Add deployment checks that verify NODE_ENV in production infrastructure.

---

### MEDIUM-02: Debug Endpoints Expose Sensitive Information

**Severity**: MEDIUM
**CVSS Score**: 4.8 (Medium)
**Files**:
- `packages/zero-cache/src/services/statz.ts`
- `packages/zero-cache/src/services/heapz.ts`
- `packages/zero-cache/src/server/runner/zero-dispatcher.ts:24-29`

**Vulnerability Description**:

Two debug endpoints are always registered on the HTTP server:

```typescript
// zero-dispatcher.ts:24-29
fastify.get('/statz', (req, res) =>
  handleStatzRequest(lc, config, req, res),
);
fastify.get('/heapz', (req, res) =>
  handleHeapzRequest(lc, config, req, res),
);
```

**Information Exposed by `/statz`**:

| Category | Information Exposed |
|----------|---------------------|
| Upstream DB | Number of replicas, clients, mutations |
| CVR | Query counts, client group IDs, AST sizes |
| Change DB | Change log size |
| Replica | WAL state, page count, journal mode, file stats |
| OS | Load average, uptime, memory, CPU count, platform, arch |

**Information Exposed by `/heapz`**:

- Full V8 heap snapshot
- All objects in memory including:
  - Configuration values
  - Active connection states
  - Cached credentials/tokens
  - Database query results

**Risk Assessment**:
- Requires admin password in production (positive)
- Admin password comparison is timing-vulnerable (see Phase 3.2)
- Development mode bypasses authentication entirely (MEDIUM-01)
- No rate limiting on authentication attempts

**Recommendation**:

1. Add option to disable debug endpoints entirely in production:
```typescript
if (config.disableDebugEndpoints) {
  return;
}
fastify.get('/statz', ...);
```

2. Add rate limiting to prevent brute-force attacks:
```typescript
const rateLimiter = new RateLimiter({
  windowMs: 60_000,
  max: 5,  // 5 attempts per minute
});
```

3. Consider moving sensitive endpoints to a separate admin port:
```typescript
// Separate admin server on different port
const adminServer = createAdminServer(config.adminPort);
adminServer.get('/statz', ...);
```

---

### LOW-01: Heap Snapshot File Left on Disk Briefly

**Severity**: LOW
**CVSS Score**: 2.3 (Low)
**File**: `packages/zero-cache/src/services/heapz.ts:24-38`

**Vulnerability Description**:

The heap snapshot is written to disk before streaming, creating a temporary file:

```typescript
const filename = v8.writeHeapSnapshot();  // Writes to disk
const stream = fs.createReadStream(filename);
void res.send(stream);

stream.on('end', () => {
  fs.unlink(filename, err => {...});  // Deleted after stream ends
});
```

**Impact**:
- Heap snapshot exists on disk during streaming
- If process crashes during streaming, file may persist
- File permissions depend on umask (may be readable by others)

**Recommendation**:
1. Write to a dedicated temp directory with restrictive permissions
2. Add cleanup on process exit/crash
3. Consider streaming directly without intermediate file

---

## Positive Findings

### Connection Strings Not Logged

**Status**: SECURE
**Files**: Various

Database connection strings are handled securely:

1. **Sanitized logging**: Uses `hostPort(upstream)` to extract only host:port
2. **No direct credential logging**: Searched for patterns that would log connection strings - none found
3. **Standard logging**: `normalize.ts:106` logs only `taskID` and `hostIP`

```typescript
// decommission.ts:67-69
function hostPort(db: PostgresDB) {
  const {host, port} = db.options;
  return `${host.join(',')}:${port?.at(0) ?? 5432}`;
}
```

### Production Mode Defaults

**Status**: SECURE
**File**: `packages/zero-cache/src/config/normalize.ts:27-29`

Production mode is the default when NODE_ENV is not set:

```typescript
export function isDevelopmentMode(): boolean {
  return process.env.NODE_ENV === 'development';  // Must be explicitly set
}
```

This means:
- Unset NODE_ENV = production behavior
- Empty NODE_ENV = production behavior
- Only explicit `NODE_ENV=development` enables development mode

### Admin Password Enforcement

**Status**: SECURE
**File**: `packages/zero-cache/src/config/normalize.ts:42-47`

Production mode requires admin password at startup:

```typescript
if (!isDevelopmentMode()) {
  assert(
    config.adminPassword,
    'missing --admin-password: required in production mode',
  );
}
```

This prevents starting in production without an admin password.

### Configuration Validation

**Status**: ADEQUATE
**File**: `packages/zero-cache/src/config/zero-config.ts`

Configuration uses valita schema validation:
- Required fields are validated at parse time
- Type checking ensures correct value types
- Custom assertions for format validation (e.g., App ID characters)

---

## Development vs Production Behavior Summary

| Feature | Development Mode | Production Mode |
|---------|------------------|-----------------|
| Admin Password | Optional | Required |
| /statz, /heapz Access | No password needed | Requires admin password |
| Inspector Authentication | Bypassed | Required |
| JSON Value Assertions | Enabled | Skipped (performance) |
| Default Behavior | Must set NODE_ENV=development | Default |

---

## Environment Variable Security

### Sensitive Environment Variables

| Variable | Contains | Risk if Logged |
|----------|----------|----------------|
| ZERO_UPSTREAM_DB | Database connection string with credentials | HIGH |
| ZERO_CVR_DB | Database connection string | HIGH |
| ZERO_CHANGE_DB | Database connection string | HIGH |
| ZERO_AUTH_SECRET | JWT signing secret | HIGH |
| ZERO_ADMIN_PASSWORD | Admin endpoint password | MEDIUM |
| ZERO_MUTATE_API_KEY | API key for mutations | MEDIUM |

### Logging Analysis

No evidence found of sensitive environment variables being logged:
- Connection strings: Sanitized via `hostPort()`
- Passwords: Not logged (only validation result logged)
- API keys: Stored in config but not logged

---

## Remediation Priority

| Finding | Severity | Effort | Priority |
|---------|----------|--------|----------|
| MEDIUM-01: Development mode auth bypass | Medium | Low | P1 |
| MEDIUM-02: Debug endpoints info exposure | Medium | Medium | P2 |
| LOW-01: Heap snapshot temp file | Low | Low | P3 |

---

## Recommended Test Cases

### For MEDIUM-01 (Development Mode Bypass):
```typescript
test('rejects admin access in production without password', async () => {
  process.env.NODE_ENV = 'production';
  const config = { adminPassword: undefined };

  // Should throw at startup
  expect(() => assertNormalized(config)).toThrow('missing --admin-password');
});

test('rejects /statz in production without credentials', async () => {
  process.env.NODE_ENV = 'production';
  const response = await fetch('/statz');
  expect(response.status).toBe(401);
});
```

### For MEDIUM-02 (Debug Endpoints):
```typescript
test('/statz does not expose connection strings', async () => {
  const response = await fetch('/statz', {
    headers: { Authorization: 'Basic ' + btoa(':adminpass') }
  });
  const body = await response.text();

  expect(body).not.toContain('postgres://');
  expect(body).not.toContain('password');
  expect(body).not.toContain('@');  // URI password separator
});
```

---

## Files Reviewed

| File | Purpose | Findings |
|------|---------|----------|
| `packages/zero-cache/src/config/zero-config.ts` | Main config definitions | Dev mode bypass |
| `packages/zero-cache/src/config/normalize.ts` | Config normalization | isDevelopmentMode() |
| `packages/zero-cache/src/services/statz.ts` | Statistics endpoint | Info exposure |
| `packages/zero-cache/src/services/heapz.ts` | Heap dump endpoint | Temp file issue |
| `packages/zero-cache/src/server/runner/zero-dispatcher.ts` | Route registration | Always registers debug routes |
| `packages/zero-cache/src/server/inspector-delegate.ts` | Inspector auth | Dev mode bypass |
| `packages/zero-cache/src/types/pg.ts` | Postgres client | No credential logging |
| `packages/zero-cache/src/scripts/decommission.ts` | DB cleanup | Sanitized logging |

---

## Appendix: NODE_ENV Detection Logic

```
┌─────────────────────────┐
│   NODE_ENV Check        │
└───────────┬─────────────┘
            │
            ▼
    ┌───────────────┐
    │ NODE_ENV set? │
    └───────┬───────┘
            │
       ┌────┴────┐
       │         │
       ▼         ▼
   ┌──────┐  ┌──────────┐
   │ Yes  │  │ No/Empty │
   └──┬───┘  └────┬─────┘
      │           │
      ▼           ▼
┌───────────┐ ┌────────────┐
│ == 'dev'? │ │ Production │
└─────┬─────┘ │   Mode     │
      │       └────────────┘
   ┌──┴──┐
   │     │
   ▼     ▼
┌─────┐ ┌────────────┐
│ Dev │ │ Production │
│Mode │ │   Mode     │
└─────┘ └────────────┘
```
