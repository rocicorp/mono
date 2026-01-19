# Zero-Protocol Security Review

**Date:** 2026-01-19
**Scope:** `packages/zero-protocol`, `packages/zero-cache`, `packages/zero-client`
**Reviewer:** Claude Code (Security Analysis)

---

## Executive Summary

The zero-protocol is reasonably well-designed from a security perspective, with strong input validation, parameterized queries, and proper authorization hooks. However, several security findings were identified ranging from high to informational severity.

**Overall Risk Assessment:** Medium

| Severity | Count |
|----------|-------|
| High | 1 |
| Medium | 3 |
| Low | 2 |

---

## HIGH SEVERITY

### 1. Sensitive Data Logging (Auth Tokens)

**Location:** `packages/zero-cache/src/workers/syncer.ts:169-171`

```typescript
this.#lc.debug?.(
  `Received auth token ${auth} for clientID ${clientID}, decoded: ${JSON.stringify(decodedToken)}`,
);
```

**Description:** Raw JWT tokens AND their decoded payloads are logged to debug output. If debug logging is enabled in production or logs are exposed, this leaks:
- Full authentication tokens that could be replayed
- JWT claims potentially containing PII or sensitive authorization data

**Impact:** Token theft enabling session hijacking; PII exposure in logs.

**Recommendation:**
- Redact tokens in logs (show only last 8 characters or a hash)
- Exclude sensitive claims from logged payloads
- Consider using a structured logging approach that automatically redacts sensitive fields

---

## MEDIUM SEVERITY

### 2. User-Provided API Endpoint URLs (Potential SSRF)

**Location:** `packages/zero-protocol/src/connect.ts:33-37`

```typescript
userPushURL: v.string().optional(),
userPushHeaders: v.record(v.string()).optional(),
userQueryURL: v.string().optional(),
userQueryHeaders: v.record(v.string()).optional(),
```

**Description:** Clients can specify custom URLs for mutation and query processing via the `initConnection` message. While URL pattern validation exists in `packages/zero-cache/src/custom/fetch.ts:78-96`, this is a powerful feature that could enable Server-Side Request Forgery (SSRF) if misconfigured.

**Mitigations Found:**
- URLPattern matching against `ZERO_MUTATE_URL` / `ZERO_QUERY_URL` configuration
- Requests only made if patterns are explicitly configured

**Impact:** If URL patterns are misconfigured (e.g., overly permissive wildcards), attackers could:
- Redirect server requests to internal services
- Exfiltrate data to attacker-controlled endpoints
- Probe internal network infrastructure

**Recommendation:**
- Documentation should emphasize never using wildcard patterns like `https://*`
- Restrict patterns to specific known-good hosts
- Consider adding explicit allowlist validation in addition to pattern matching
- Log and alert on user-provided URLs that differ from server defaults

---

### 3. Cookie/Credential Forwarding to User-Specified Endpoints

**Location:** `packages/zero-cache/src/custom/fetch.ts:107-112`

```typescript
if (headerOptions.token) {
  headers['Authorization'] = `Bearer ${headerOptions.token}`;
}
if (headerOptions.cookie) {
  headers['Cookie'] = headerOptions.cookie;
}
```

**Description:** HTTP cookies and auth tokens from the client WebSocket connection are forwarded to user-specified mutation/query endpoints. Combined with finding #2, this creates a credential exfiltration risk.

**Impact:** If an attacker can control the push/query URL (via misconfigured URL patterns), cookies and tokens would be forwarded to the attacker's server.

**Recommendation:**
- Make `forwardCookies: false` the secure default, requiring explicit opt-in
- Add configuration option to disable credential forwarding entirely
- Log warnings when credentials are forwarded to non-default URLs

---

### 4. Inspect/Debug Features Accessible Without Elevated Auth

**Location:** `packages/zero-protocol/src/inspect-up.ts`

**Description:** The protocol supports inspection operations accessible over the standard WebSocket connection:
- `authenticate` - Test authentication tokens
- `analyze-query` - Analyze query execution plans and costs
- `metrics` - Server performance metrics (TDigest-based)
- `queries` - List active queries with details

While read authorization applies to data queries, these debug operations could leak:
- Schema structure and table names
- Query patterns and performance characteristics
- Server resource utilization

**Impact:** Information disclosure that aids reconnaissance for further attacks.

**Recommendation:**
- Require elevated permissions or admin authentication for inspect operations in production
- Consider making inspect features opt-in via server configuration
- Add rate limiting to prevent enumeration attacks

---

## LOW SEVERITY

### 5. Error Message Information Disclosure

**Location:** `packages/zero-cache/src/workers/syncer.ts:176-179`

```typescript
sendError(
  this.#lc,
  ws,
  {
    kind: ErrorKind.AuthInvalidated,
    message: `Failed to decode auth token: ${String(e)}`,
    origin: ErrorOrigin.ZeroCache,
  },
  e,
);
```

**Description:** JWT parsing errors are sent back to clients with full error details, which could leak:
- Expected token structure and format
- Signing algorithm requirements
- Key configuration details

**Impact:** Aids attackers in crafting valid-looking tokens or understanding auth configuration.

**Recommendation:**
- Return generic "Invalid authentication token" message to clients
- Log detailed errors server-side for debugging
- Consider using error codes instead of detailed messages

---

### 6. Admin Endpoints Unprotected in Development Mode

**Location:** `packages/zero-cache/src/config/zero-config.ts:896-901`

```typescript
if (!password && !config.adminPassword && isDevelopmentMode()) {
  warnOnce(
    lc,
    'No admin password set; allowing access in development mode only',
  );
  return true;
}
```

**Description:** The `/heapz` endpoint (and potentially others) require no password when `NODE_ENV=development`. The `/heapz` endpoint generates V8 heap snapshots which can contain sensitive runtime data including:
- In-memory credentials and tokens
- User data being processed
- Internal application state

**Impact:** If deployed with `NODE_ENV=development` by mistake, admin endpoints expose sensitive server internals.

**Recommendation:**
- Add explicit startup warnings if admin endpoints are unprotected
- Consider requiring admin password even in development mode
- Add production deployment checklist to documentation

---

## POSITIVE FINDINGS

### SQL Injection Protection

The codebase properly uses parameterized queries throughout:

```typescript
// packages/zero-cache/src/auth/write-authorizer.ts:458,464-467
conditions.push(sql`${sql.ident(pk)}=?`);
// ...
sql`SELECT ${sql.join(
  Object.keys(spec.zqlSpec).map(c => sql.ident(c)),
  sql`,`,
)} FROM ${sql.ident(op.tableName)} WHERE ${sql.join(
  conditions,
  sql` AND `,
)}`
```

- `sql.ident()` used for identifier escaping
- Value binding via `?` placeholders
- No `sql.raw()` usage found in security-critical paths
- AST-based query transformation prevents string injection

### Input Validation

All protocol messages are validated against strict Valita schemas:

```typescript
// packages/zero-cache/src/workers/connection.ts:186-187
const value = JSON.parse(data);
msg = valita.parse(value, upstreamSchema);
```

- Invalid messages trigger immediate connection termination
- Strong typing enforced through schema inference
- No unsafe deserialization (eval, new Function, etc.)

### Authorization Architecture

**Read Authorization** (`packages/zero-cache/src/auth/read-authorizer.ts`):
- Permission rules injected into query WHERE clauses via AST transformation
- Oracle attack prevention by applying read policies to WHERE subqueries (lines 121-151)
- Static auth data bound at query build time

**Write Authorization** (`packages/zero-cache/src/auth/write-authorizer.ts`):
- Pre-mutation checks (before write) validate permissions
- Post-mutation checks (after write) verify final state
- Table, column, row, and cell-level authorization supported

### Auth Token Transport Security

Tokens are passed via WebSocket `Sec-WebSocket-Protocol` header, not URL parameters:

```typescript
// packages/zero-protocol/src/connect.ts:58-76
export function encodeSecProtocols(
  initConnectionMessage: InitConnectionMessage | undefined,
  authToken: string | undefined,
): string {
  const protocols = { initConnectionMessage, authToken };
  const bytes = new TextEncoder().encode(JSON.stringify(protocols));
  const s = Array.from(bytes, byte => String.fromCharCode(byte)).join('');
  return encodeURIComponent(btoa(s));
}
```

- Multi-layer encoding (JSON -> UTF-8 -> Base64 -> URI)
- Prevents token exposure in URL bars and access logs

### Connection Security

- JWT verification using `jose` library (industry standard)
- Protocol version negotiation prevents downgrade attacks
- Server-side heartbeats detect and terminate dead connections
- Proper connection lifecycle management with cleanup on close/error

---

## Recommendations Summary

| Priority | Issue | Action |
|----------|-------|--------|
| HIGH | Auth token logging | Redact tokens and sensitive claims from logs |
| MEDIUM | User-provided URLs | Strengthen documentation; consider strict allowlist |
| MEDIUM | Cookie forwarding | Make `forwardCookies: false` the default |
| MEDIUM | Inspect operations | Require admin auth for debug endpoints in production |
| LOW | Error messages | Use generic auth error messages for clients |
| LOW | Dev mode access | Add startup warnings for unprotected admin endpoints |

---

## Files Reviewed

### Core Protocol
- `packages/zero-protocol/src/connect.ts` - Connection handshake and auth encoding
- `packages/zero-protocol/src/up.ts` - Upstream (client->server) message types
- `packages/zero-protocol/src/down.ts` - Downstream (server->client) message types
- `packages/zero-protocol/src/ast.ts` - Query AST serialization format
- `packages/zero-protocol/src/push.ts` - Mutation message format
- `packages/zero-protocol/src/error.ts` - Error types and ProtocolError class
- `packages/zero-protocol/src/inspect-up.ts` - Debug/inspection operations

### Zero Cache (Server)
- `packages/zero-cache/src/workers/syncer.ts` - WebSocket connection handling
- `packages/zero-cache/src/workers/connection.ts` - Message processing
- `packages/zero-cache/src/auth/read-authorizer.ts` - Query permission transformation
- `packages/zero-cache/src/auth/write-authorizer.ts` - Mutation authorization
- `packages/zero-cache/src/custom/fetch.ts` - External API fetching with URL validation
- `packages/zero-cache/src/services/heapz.ts` - Heap snapshot endpoint
- `packages/zero-cache/src/config/zero-config.ts` - Configuration and admin auth

### Zero Client
- `packages/zero-client/src/client/zero.ts` - Main client implementation
- `packages/zero-client/src/util/socket.ts` - WebSocket message sending
- `packages/zero-client/src/client/connection.ts` - Connection state management