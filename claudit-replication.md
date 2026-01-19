# Security Audit Report: Zero-Cache Replication

## Critical Findings

### 1. SQL Injection via Snapshot ID Interpolation (HIGH)

**Location:** `packages/zero-cache/src/db/transaction-pool.ts:554`, `initial-sync.ts:139`

```typescript
await tx.unsafe(/* sql*/ `SET TRANSACTION SNAPSHOT '${snapshot}'`);
await tx.unsafe(`SET TRANSACTION SNAPSHOT '${snapshotID}'`);
```

The `snapshotID` variable comes from PostgreSQL's `CREATE_REPLICATION_SLOT` response and is directly interpolated into SQL. While currently controlled by upstream PostgreSQL, a compromised or malicious upstream could potentially inject SQL via a crafted snapshot name.

**Risk:** If an attacker controls the upstream database, they could inject arbitrary SQL.

---

### 2. Authentication Bypass via Custom Endpoints (HIGH)

**Location:** `packages/zero-cache/src/workers/syncer.ts:154-162`

```typescript
const hasCustomEndpoints = hasPushOrMutate && hasQueries;
if (!hasExactlyOneTokenOption && !hasCustomEndpoints) {
  throw new Error(...);
}
// If hasCustomEndpoints is true, token validation is SKIPPED
if (tokenOptions.length > 0) {
  decodedToken = await verifyToken(...);
} else {
  this.#lc.warn?.(`One of jwk, secret, or jwksUrl is not configured...`);
}
```

When both `ZERO_MUTATE_URL` and `ZERO_QUERY_URL` are configured, JWT token verification is bypassed. An attacker could configure these endpoints and gain unauthenticated access. The `auth` token is passed through but never verified, and the `decodedToken` remains `undefined`.

---

### 3. Admin Endpoint Unprotected in Development Mode (MEDIUM)

**Location:** `packages/zero-cache/src/config/zero-config.ts:896-902`

```typescript
if (!password && !config.adminPassword && isDevelopmentMode()) {
  warnOnce(lc, 'No admin password set; allowing access in development mode only');
  return true;
}
```

The `/heapz` and `/statz` endpoints are accessible without authentication when `NODE_ENV=development`. If a production deployment accidentally has `NODE_ENV=development` set, sensitive heap snapshots and database statistics become exposed.

---

### 4. Unsanitized JSON Parsing in Protocol Decoder (MEDIUM)

**Location:** `packages/zero-protocol/src/connect.ts:79-86`

```typescript
export function decodeSecProtocols(secProtocol: string) {
  const binString = atob(decodeURIComponent(secProtocol));
  const bytes = Uint8Array.from(binString, c => c.charCodeAt(0));
  return JSON.parse(new TextDecoder().decode(bytes)); // No schema validation!
}
```

The returned object is not validated against a schema. In `connect-params.ts:50-52`, the result is destructured and used directly:

```typescript
const {initConnectionMessage, authToken} = decodeSecProtocols(
  must(headers['sec-websocket-protocol']),
);
```

An attacker could inject unexpected properties that may be consumed elsewhere.

---

### 5. Heap Snapshot Information Disclosure (MEDIUM)

**Location:** `packages/zero-cache/src/services/heapz.ts:24`

```typescript
const filename = v8.writeHeapSnapshot();
```

Heap snapshots contain all in-memory data including:
- Database connection strings
- JWT secrets
- User data currently being processed
- Internal state

Combined with the development mode bypass, this is a significant data leak vector.

---

### 6. Replication Slot Name SQL Injection Vector (LOW-MEDIUM)

**Location:** `packages/zero-cache/src/services/change-source/pg/initial-sync.ts:345`

```typescript
await session.unsafe<ReplicationSlot[]>(
  /*sql*/ `CREATE_REPLICATION_SLOT "${slotName}" LOGICAL pgoutput`,
)
```

The `slotName` is constructed from the app ID. While there's validation at `initial-sync.ts:67`:

```typescript
if (!ALLOWED_APP_ID_CHARACTERS.test(shard.appID)) {
  throw new Error(...);
}
```

The regex `ALLOWED_APP_ID_CHARACTERS` should be verified to exclude all SQL injection characters including quotes.

---

### 7. Permission Loading SQL with String Interpolation (MEDIUM)

**Location:** `packages/zero-cache/src/auth/load-permissions.ts:27,77`

```typescript
const {permissions, hash} = replica.get(
  `SELECT permissions, hash FROM "${appID}.permissions"`,
);
```

The `appID` is interpolated into SQL. While validated elsewhere, if validation is bypassed (e.g., internal API misuse), SQL injection is possible.

---

### 8. Information Disclosure via `/statz` Endpoint (LOW)

**Location:** `packages/zero-cache/src/services/statz.ts`

The endpoint exposes:
- Database connection counts
- Client group counts and mutation statistics
- Query AST sizes and hashes
- Replica file statistics
- OS information (load, memory, platform)

This information could help attackers understand the system architecture and identify targets.

---

### 9. Binary Protocol Parsing Without Bounds Checking Edge Cases (LOW)

**Location:** `packages/zero-cache/src/services/change-source/pg/logical-replication/binary-reader.ts`

The `readInt32()` method uses bit shifting on signed integers:

```typescript
readInt32() {
  return (
    (this.#b[this.#p++] << 24) |
    (this.#b[this.#p++] << 16) |
    (this.#b[this.#p++] << 8) |
    this.#b[this.#p++]
  );
}
```

For values with the high bit set, this produces negative numbers in JavaScript, which may cause unexpected behavior when used as array indices or lengths in `array()` calls.

---

### 10. Missing Rate Limiting on Change Streamer Subscriptions (LOW)

**Location:** `packages/zero-cache/src/services/change-streamer/change-streamer-http.ts`

There's no rate limiting on WebSocket subscription requests to `/replication/v*/changes`. An attacker could open many connections to exhaust server resources.

---

## Security Recommendations

1. **Use parameterized queries** for all SQL, including `SET TRANSACTION SNAPSHOT` commands
2. **Require explicit authentication configuration** - remove the "custom endpoints bypass auth" logic
3. **Never allow password-less admin access** regardless of `NODE_ENV`
4. **Add schema validation** to `decodeSecProtocols()` output using valita
5. **Rate limit** the change streamer WebSocket endpoints
6. **Audit `sql.unsafe()` usage** - 24 occurrences found, many in production code
7. **Add IP allowlisting** for admin endpoints
8. **Sanitize or validate** all interpolated values in SQL strings even if they come from "trusted" sources

---

## Summary

| Severity | Count | Description |
|----------|-------|-------------|
| HIGH | 2 | SQL injection via snapshot ID, Auth bypass with custom endpoints |
| MEDIUM | 4 | Dev mode admin bypass, unvalidated JSON, heap disclosure, permission SQL |
| LOW | 4 | Slot name injection, statz disclosure, binary parsing, rate limiting |

The most critical issue is the **authentication bypass when custom endpoints are configured** (finding #2), as it allows unauthenticated clients to connect and potentially access data based only on the permission rules, which may not expect unauthenticated access.