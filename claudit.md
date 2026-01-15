# Zero Sync Platform Security Assessment Report

**Assessment Date:** January 2026
**Assessor:** Claude (Code Review)
**Scope:** Zero Cache, Zero Server, ZQL Query Engine, Protocol Layer
**Methodology:** Static code analysis, architecture review, threat modeling

---

## Executive Summary

This security assessment evaluated the Zero sync platform, a real-time data synchronization system built on PostgreSQL/SQLite with client-side caching via Replicache. The assessment focused on authentication, authorization, input validation, and injection vulnerabilities.

### Risk Summary

| Risk Level | Count | Description |
|------------|-------|-------------|
| **CRITICAL** | 2 | Immediate action required |
| **HIGH** | 6 | Should be addressed soon |
| **MEDIUM** | 5 | Should be evaluated |
| **LOW** | 0 | Acceptable risk |

### Key Findings

| ID | Finding | Severity | Status |
|----|---------|----------|--------|
| ZSP-001 | Development Mode Admin Access | ✅ By Design | N/A |
| ZSP-002 | Custom Endpoint JWT Verification Skip | ✅ By Design | N/A |
| ZSP-003 | Weak SSRF Protection | **HIGH** | Open |
| ZSP-004 | SQL Injection Protections | ✅ Secure | N/A |
| ZSP-005 | `internalQuery` RLS Bypass | ✅ Secure | N/A |
| ZSP-006 | Default Permission Model | ✅ Secure | N/A |
| ZSP-007 | Mutation Path Security | ✅ Secure | N/A |
| ZSP-008 | Query Path Security | ✅ Secure | N/A |
| ZSP-009 | No Origin Header Validation | **HIGH** | Open |
| ZSP-010 | No WebSocket Message Size Limits | **HIGH** | Open |
| ZSP-011 | No Connection Rate Limiting | **HIGH** | Open |
| ZSP-012 | Non-Mutation Message Flooding | **MEDIUM** | Open |
| ZSP-013 | Sensitive Data in Error Messages | **MEDIUM** | Open |
| ZSP-014 | wsID Default Empty String | **MEDIUM** | Verify |
| ZSP-015 | /heapz Authentication Bypass | **CRITICAL** | Open |
| ZSP-016 | Unauthenticated Replication Endpoints | **CRITICAL** | Open |
| ZSP-017 | No Shard-Level Authorization | **HIGH** | Open |
| ZSP-018 | SQLite Replica Unencrypted | **MEDIUM** | Open |
| ZSP-019 | No Server-to-Server Authentication (zero-server) | **HIGH** | Open |
| ZSP-020 | Direct API Bypass of zero-cache | **MEDIUM** | Open |
| ZSP-021 | Zero-Server SQL Injection Protection | ✅ Secure | N/A |
| ZSP-022 | Zero-Server Mutation Idempotency | ✅ Secure | N/A |

---

## Detailed Findings

### ZSP-001: Development Mode Admin Access

**Status:** ✅ BY DESIGN

#### Description

When `NODE_ENV=development`, admin endpoints (`/statz`, `/heapz`, inspector) are accessible without password authentication. This is intentional behavior for local development convenience.

#### Security Model

- **Development mode:** Password optional (local development only)
- **Production mode:** Password required (enforced via startup assertion)

#### Key Files

| File | Lines | Function |
|------|-------|----------|
| `packages/zero-cache/src/config/normalize.ts` | 27-29 | `isDevelopmentMode()` |
| `packages/zero-cache/src/config/zero-config.ts` | 885-916 | `isAdminPasswordValid()` |

#### Rationale

This design follows common Node.js conventions where `NODE_ENV=development` indicates a local development environment. Production deployments should never set `NODE_ENV=development`, and the startup assertion enforces admin password requirement in production mode.

---

### ZSP-002: Custom Endpoint JWT Verification Delegation

**Status:** ✅ BY DESIGN

#### Description

When custom mutation/query URLs are configured (`ZERO_MUTATE_URL` + `ZERO_QUERY_URL`) without JWT verification options, the Zero Cache server forwards raw authentication tokens to the user's external API server without local verification. This is intentional delegation of authentication responsibility.

#### Security Model

| Configuration | JWT Verification Location | Mutations Protected By |
|--------------|---------------------------|------------------------|
| Custom endpoints only | External API server | User's API server |
| Custom + JWT config | Zero layer + External API | Both layers |
| Legacy only | Zero layer (required) | Zero permission system |

#### Key Files

| File | Lines | Function |
|------|-------|----------|
| `packages/zero-cache/src/workers/syncer.ts` | 153-190 | `#createConnection()` |
| `packages/zero-cache/src/services/mutagen/pusher.ts` | 110-124 | `enqueuePush()` |

#### Why This Is Secure

1. **Custom mutations cannot execute locally** - Zero Cache cannot apply changes without external API approval
2. **External API is the authorization boundary** - Response required before any mutation takes effect
3. **Tokens forwarded with request** - External API receives full auth context for validation
4. **Legacy mutations remain protected** - Permission system with default-deny still applies to CRUD operations

#### Rationale

This design allows users to implement their own authentication logic in their API server, supporting custom auth schemes, session management, and business logic that Zero cannot anticipate. The external API server is responsible for validating tokens and returning appropriate errors for unauthorized requests.

---

### ZSP-003: Weak SSRF Protection

**Severity:** HIGH
**CVSS Score:** 7.5 (High)
**CWE:** CWE-918 (Server-Side Request Forgery)

#### Description

The URL validation for custom endpoints uses only `URLPattern` matching, which does not prevent requests to internal network resources, localhost, or cloud metadata services.

#### Affected Components

| File | Lines | Function |
|------|-------|----------|
| `packages/zero-cache/src/custom/fetch.ts` | 248-258 | `urlMatch()` |

#### Vulnerable Code

```typescript
// packages/zero-cache/src/custom/fetch.ts:248-258
export function urlMatch(url: string, allowedUrlPatterns: URLPattern[]): boolean {
  for (const pattern of allowedUrlPatterns) {
    if (pattern.test(url)) {  // URL pattern matching only
      return true;
    }
  }
  return false;
}
```

#### Missing Protections

- No validation against localhost (`127.0.0.1`, `localhost`, `::1`)
- No validation against private IP ranges (`10.x.x.x`, `172.16-31.x.x`, `192.168.x.x`)
- No validation against cloud metadata endpoints (`169.254.169.254`)
- No DNS rebinding protection

#### Impact

- Access to internal services not exposed to internet
- Cloud credential theft via metadata service
- Internal network reconnaissance

#### Remediation

1. **IP Address Validation:** Resolve hostname and validate against blocklist
2. **DNS Rebinding Protection:** Re-validate IP after DNS resolution
3. **Allowlist-only mode:** Require explicit IP ranges for external endpoints

---

### ZSP-007: Mutation Path Security

**Status:** ✅ SECURE

#### Architecture

Zero has two distinct mutation paths with separate authorization mechanisms:

| Path | Handler | Authorization | Token Handling |
|------|---------|---------------|----------------|
| **Legacy (CRUD)** | Mutagen service | Zero's permission system | Validated at Zero layer (if configured) |
| **Custom** | Pusher → External API | User's API server | Forwarded to external API (delegated) |

#### Legacy Mutation Security

Token flow: `Client → syncer.ts → SyncerWsMessageHandler → Mutagen.processMutation()`

**Key Files:**
- `packages/zero-cache/src/services/mutagen/mutagen.ts:336-340` - Authorization enforcement
- `packages/zero-cache/src/auth/write-authorizer.ts:516-521` - Default deny logic

**Security Properties:**
- Permission checks (`canPre`/`canPost`) always enforced
- Default deny when no policies defined
- authData fields resolve to undefined when token missing → policy failures → DENY

#### Custom Mutation Security

Token flow: `Client → syncer-ws-message-handler.ts → Pusher.enqueuePush() → External API`

**Key Files:**
- `packages/zero-cache/src/workers/syncer-ws-message-handler.ts:121-134` - Routing logic
- `packages/zero-cache/src/services/mutagen/pusher.ts:110-124` - Token forwarding

**Security Properties:**
- Zero Cache cannot apply custom mutations locally
- External API response required for mutation effect
- Authorization delegated to user's API server (by design)

#### Mixed Mode Security

**Concern:** Can CRUD mutations bypass custom auth when both are configured?

**Finding:** No bypass possible because:
1. CRUD mutations have their own authorization (Zero's permission system)
2. Permission system defaults to DENY when no rules defined
3. Each mutation type uses its own independent authorization path

#### Token Validation Matrix

| JWT Config | Custom Endpoints | Legacy CRUD Auth | Custom Mutation Auth |
|------------|------------------|------------------|---------------------|
| Configured | No | Permission system | N/A |
| Configured | Yes | Permission system | External API |
| Not configured | Yes | Permission system (default deny) | External API |

---

### ZSP-008: Query Path Security

**Status:** ✅ SECURE

#### Architecture

Zero has two distinct query paths with separate authorization mechanisms:

| Path | Handler | Authorization | Token Handling |
|------|---------|---------------|----------------|
| **Legacy (Client)** | ViewSyncer + ReadAuthorizer | Zero's RLS permission system | Claims bound to WHERE conditions |
| **Custom** | CustomQueryTransformer → External API | User's API server | Forwarded to external API (delegated) |

#### Legacy Query Security

Token flow: `Client → syncer.ts → ViewSyncer → transformAndHashQuery(authData) → RLS WHERE conditions`

**Key Files:**
- `packages/zero-cache/src/auth/read-authorizer.ts:68-84` - Default deny logic
- `packages/zero-cache/src/auth/read-authorizer.ts:45-59` - authData binding to RLS

**Security Properties:**
- Default deny: No rules = empty OR condition = no rows returned
- authData claims bound directly into WHERE clauses
- Subqueries also RLS-transformed (prevents oracle attacks)
- Anonymous clients only get `ANYONE_CAN` rules

#### Custom Query Security

Token flow: `Client → ViewSyncer → CustomQueryTransformer → External API → AST → Local execution`

**Key Files:**
- `packages/zero-cache/src/custom-queries/transform-query.ts:74-125` - Token forwarding
- `packages/zero-cache/src/custom/fetch.ts:77-95` - URL validation

**Security Properties:**
- External API decides what data to expose via returned AST
- AST executed locally against SQLite replica (not at external API)
- URL allowlist validation prevents SSRF
- Response schema validation rejects invalid ASTs
- Caching keyed by token (users see only their cached results)

#### Query Routing Security

**File:** `packages/zero-cache/src/services/view-syncer/cvr.ts:1020-1050`

Query type is **server-determined** based on field presence:
- `ast` present → client query (RLS applied)
- `name/args` present → custom query (external API)
- Both present → assertion failure (rejected)

#### Internal Query Protection

**File:** `packages/zero-cache/src/services/view-syncer/cvr.ts:82-89`

Internal queries (which bypass RLS) can ONLY be created by the system with hardcoded IDs (`lmids`, `mutationResults`). Clients cannot trigger the internal query bypass.

---

### ZSP-009: No Origin Header Validation

**Severity:** HIGH

#### Description

The WebSocket server does NOT validate the Origin header during upgrade. No CORS configuration is implemented.

#### Affected Components

| File | Purpose |
|------|---------|
| `packages/zero-cache/src/server/worker-dispatcher.ts` | WebSocket routing |
| `packages/zero-cache/src/services/http-service.ts` | HTTP/Fastify config |

#### Impact

Attackers from malicious origins can establish WebSocket connections. Could enable CSRF-style attacks if combined with session cookies.

#### Mitigation

The server requires explicit authentication tokens (when configured), providing defense in depth.

---

### ZSP-010: No WebSocket Message Size Limits

**Severity:** HIGH

#### Description

No `maxPayload` is configured on the WebSocket server, allowing arbitrarily large messages.

#### Affected Components

**File:** `packages/zero-cache/src/workers/syncer.ts:40-63`

```typescript
function getWebSocketServerOptions(config: ZeroConfig): ServerOptions {
  const options: ServerOptions = {
    noServer: true,  // No maxPayload specified
  };
}
```

#### Impact

Memory exhaustion via oversized messages or compression bombs (if perMessageDeflate enabled).

#### Remediation

Add `maxPayload: 1024 * 1024` (1MB) to WebSocket server options.

---

### ZSP-011: No Connection Rate Limiting

**Severity:** HIGH

#### Description

No rate limiting on WebSocket connection attempts:
- No per-IP throttling
- No per-clientID connection limits
- Only mutation-level rate limiting exists

#### Impact

DoS via connection flooding. Attackers can exhaust server resources by opening many connections.

---

### ZSP-012: Non-Mutation Message Flooding

**Severity:** MEDIUM

#### Description

Rate limiting only applies to mutations. Unprotected message types:
- `changeDesiredQueries` - can be flooded
- `deleteClients` - can be flooded
- `ping` - not rate limited

#### Impact

Resource exhaustion via non-mutation message flooding.

---

### ZSP-013: Sensitive Data in Error Messages

**Severity:** MEDIUM

#### Description

**File:** `packages/zero-cache/src/workers/connection.ts:189`

```typescript
this.#lc.warn?.(`failed to parse message "${data}": ${String(e)}`);
this.#closeWithError({
  kind: ErrorKind.InvalidMessage,
  message: String(e),  // Raw exception exposed
});
```

Raw exception messages are sent to clients, potentially leaking internal details.

---

### ZSP-014: wsID Default Empty String

**Severity:** MEDIUM

#### Description

**File:** `packages/zero-cache/src/workers/connect-params.ts:47`

```typescript
const wsID = params.get('wsid', false) ?? '';
```

If no wsID provided, it defaults to empty string. Multiple connections could share the same empty wsID.

#### Mitigation

Client likely generates wsID, and server validates wsID matches on every message via two-factor validation (wsID + clientID).

---

## WebSocket Secure Components

### ✅ Message Schema Validation

All incoming messages validated against `upstreamSchema` (Valita). Invalid messages trigger connection close.

### ✅ wsID + clientID Two-Factor Validation

**File:** `packages/zero-cache/src/services/view-syncer/view-syncer.ts:870-876`

Every message validated against BOTH `clientID` AND `wsID`. Prevents connection hijacking.

### ✅ Connection Cleanup & Resource Management

Proper cleanup on close (streams canceled, timers cleared). Ref counting for service lifecycle.

### ✅ Keepalive/Timeout Handling

6-second ping/pong interval detects dead connections. Timer properly cleared on close.

---

### ZSP-015: /heapz Authentication Bypass

**Severity:** CRITICAL
**CVSS Score:** 9.1 (Critical)
**CWE:** CWE-287 (Improper Authentication)

#### Description

The `/heapz` endpoint is missing a `return` statement after sending the 401 response, allowing unauthenticated access to V8 heap snapshots.

#### Vulnerable Code

**File:** `packages/zero-cache/src/services/heapz.ts:16-21`

```typescript
if (!isAdminPasswordValid(lc, config, credentials?.pass)) {
  void res.code(401).send('Unauthorized');
  // MISSING RETURN STATEMENT - execution continues!
}

const filename = v8.writeHeapSnapshot();  // Executed even on auth failure
```

#### Impact

- **Memory Exposure:** V8 heap snapshots contain all objects in memory
- **Credential Theft:** Tokens, passwords, and API keys in memory are exposed
- **User Data Exposure:** Cached user data visible in heap
- **DoS Potential:** Repeated snapshot requests can exhaust disk and CPU

#### Remediation

Add `return;` statement after the 401 response:

```typescript
if (!isAdminPasswordValid(lc, config, credentials?.pass)) {
  void res.code(401).send('Unauthorized');
  return;  // ADD THIS LINE
}
```

---

### ZSP-016: Unauthenticated Replication Endpoints

**Severity:** CRITICAL
**CVSS Score:** 9.8 (Critical)
**CWE:** CWE-306 (Missing Authentication for Critical Function)

#### Description

The replication WebSocket endpoints have NO authentication. Anyone with network access can stream ALL database changes in real-time.

#### Affected Endpoints

| Endpoint | Purpose |
|----------|---------|
| `GET /replication/v*/changes` | Streams all database changes |
| `GET /replication/v*/snapshot` | Provides full database snapshots |

#### Vulnerable Code

**File:** `packages/zero-cache/src/services/change-streamer/change-streamer-http.ts`

```typescript
readonly #subscribe = async (ws: WebSocket, req: RequestHeaders) => {
  const ctx = getSubscriberContext(req);
  const downstream = await this.#changeStreamer.subscribe(ctx);
  // NO AUTHENTICATION CHECK - Anyone can subscribe
};
```

#### Impact

- **Complete Data Breach:** Attacker receives every INSERT, UPDATE, DELETE in real-time
- **Credential Exposure:** Database credentials and user data streamed to attacker
- **Compliance Violation:** GDPR, HIPAA, SOC2 requirements violated

#### Remediation

1. Implement authentication on replication endpoints (API key or mutual TLS)
2. Add authorization checks for shard-level access
3. Ensure replication ports are not publicly accessible

---

### ZSP-017: No Shard-Level Authorization

**Severity:** HIGH

#### Description

Replication endpoints don't validate which shards a client can access. A client can subscribe to any shard's change stream without authorization.

#### Impact

Multi-tenant deployments could have cross-tenant data leakage.

---

### ZSP-018: SQLite Replica Unencrypted

**Severity:** MEDIUM

#### Description

**File:** `packages/zero-cache/src/db/create.ts`

SQLite replica files are stored unencrypted on disk. If the container/server is compromised, all data is immediately accessible.

#### Mitigation

Consider using SQLite encryption extensions or encrypted filesystem.

---

### ZSP-019: No Server-to-Server Authentication (zero-server)

**Severity:** HIGH
**CWE:** CWE-306 (Missing Authentication for Critical Function)

#### Description

The `zero-server` package processes custom mutations and queries from `zero-cache` but has **no built-in server-to-server authentication**. Any service that can reach the zero-server HTTP endpoint can invoke mutations and queries.

#### Architecture Context

```
zero-client → zero-cache → zero-server (user's API)
                             ↑
                    NO AUTHENTICATION HERE
```

#### Affected Components

| File | Lines | Purpose |
|------|-------|---------|
| `packages/zero-server/src/process-mutations.ts` | 135-230 | Mutation entry point |
| `packages/zero-server/src/push-processor.ts` | - | Public mutation API |
| `packages/zero-server/src/queries/process-queries.ts` | - | Query entry point |

#### Code Evidence

```typescript
// packages/zero-server/src/push-processor.ts
// Mutations processed without any server-to-server auth verification
export function processPush(request: PushRequest): Promise<PushResponse> {
  // No API key validation
  // No mutual TLS verification
  // No shared secret check
}
```

#### Impact

- Any service with network access to zero-server can invoke mutations
- In cloud deployments, lateral movement could allow unauthorized mutation execution
- No audit trail of which service originated requests

#### Remediation

1. **API Key Authentication:** Add `ZERO_SERVER_API_KEY` environment variable, require in headers
2. **Mutual TLS:** Configure mTLS between zero-cache and zero-server
3. **Network Isolation:** Ensure zero-server is only accessible from zero-cache (VPC/network policy)

---

### ZSP-020: Direct API Bypass of zero-cache

**Severity:** MEDIUM
**CWE:** CWE-284 (Improper Access Control)

#### Description

Since zero-server has no authentication, attackers who discover the zero-server URL can bypass zero-cache entirely, avoiding:
- Rate limiting applied at zero-cache
- Token validation performed at zero-cache
- WebSocket connection management

#### Attack Scenario

```
Normal:  Client → zero-cache (auth + rate limit) → zero-server
Attack:  Attacker → zero-server (direct, no controls)
```

#### Impact

- Bypass of all zero-cache security controls
- Direct database manipulation via mutations
- No rate limiting on direct requests

#### Mitigation

- Applications MUST implement their own authentication in zero-server mutation handlers
- zero-server should NOT be exposed to public networks
- Use network segmentation to limit access to zero-server

---

### ZSP-021: Zero-Server SQL Injection Protection

**Status:** ✅ SECURE

#### Description

Zero-server demonstrates **excellent** SQL injection protection throughout the mutation processing pipeline.

#### Security Mechanisms

1. **Parameterized Queries:** All SQL uses parameterized queries via `@databases/sql` or similar
2. **Type-Safe Column Access:** Column names validated against schema, not passed as raw strings
3. **Valita Schema Validation:** All inputs validated before processing

#### Key Files

| File | Lines | Security Feature |
|------|-------|------------------|
| `packages/zero-server/src/custom.ts` | 350-428 | CRUD operations with safe SQL |
| `packages/z2s/src/sql.ts` | - | Parameterized SQL generation |
| `packages/zero-protocol/src/push.ts` | - | Request schema validation |

#### Code Evidence

```typescript
// packages/zero-server/src/custom.ts
// INSERT uses parameterized values - NOT string concatenation
const sql = `INSERT INTO ${sql.ident(tableName)} (${columns.map(c => sql.ident(c)).join(', ')})
             VALUES (${columns.map(() => '?').join(', ')})`;
// Values passed separately, never interpolated
```

---

### ZSP-022: Zero-Server Mutation Idempotency

**Status:** ✅ SECURE

#### Description

Zero-server correctly implements mutation idempotency guarantees, preventing duplicate processing.

#### Security Properties

1. **Mutation ID Tracking:** Each mutation has unique ID tracked in `replicache_client_group` table
2. **Last Mutation ID Validation:** Server tracks last processed mutation per client
3. **Ordering Enforcement:** Mutations processed in order, gaps detected

#### Key Files

| File | Purpose |
|------|---------|
| `packages/zero-server/src/process-mutations.ts` | Mutation ordering logic |
| `packages/zero-server/src/custom.ts` | lastMutationID tracking |

#### Impact

- Replay attacks blocked (duplicate mutations rejected)
- Client-server state consistency maintained
- Out-of-order mutations detected and handled

---

## Zero-Server Secure Components

### ✅ Prototype Pollution Prevention

**File:** `packages/shared/src/object-traversal.ts`

Mutator lookup uses safe object traversal that prevents prototype pollution attacks:
- Direct property access only
- No `__proto__` traversal
- Constructor property protected

### ✅ Schema-Based Input Validation

All mutation and query requests validated against Valita schemas before processing. Invalid inputs rejected early.

### ✅ Error Handling with Rollback

Failed mutations within a transaction trigger rollback, maintaining database consistency.

---

## Admin Endpoint Secure Components

### ✅ /statz Endpoint Properly Protected

Has correct `return` statement after auth failure (unlike /heapz).

### ✅ Admin Password Required in Production

`packages/zero-cache/src/config/normalize.ts:42-47` enforces admin password at startup.

### ✅ Secrets Handling

No hardcoded credentials. Database URIs, API keys, and passwords not logged.

---

## Secure Components

### ZSP-004: SQL Injection Protections ✅

**Status:** SECURE

The codebase demonstrates excellent SQL injection prevention:

- **Parameterized queries:** Uses `@databases/sql` library throughout
- **Identifier escaping:** `sql.ident()` properly quotes table/column names
- **AST validation:** Strict Valita schemas validate all query structures
- **No string concatenation:** SQL is built via template literals with proper escaping

**Key Files:**
- `packages/zqlite/src/internal/sql.ts`
- `packages/zqlite/src/query-builder.ts`
- `packages/zero-protocol/src/ast.ts`

---

### ZSP-005: `internalQuery` RLS Bypass ✅

**Status:** SECURE

Initial analysis identified a potential bypass where queries marked as `internalQuery: true` skip all row-level security. However, deep code review confirmed this is **not exploitable**:

1. **Client protocol has no `type` field** - Clients cannot specify query type
2. **Server hardcodes types** - `newQueryRecord()` only returns 'client' or 'custom'
3. **Assertion guards** - `assertNotInternal()` validates at all entry points
4. **Internal queries server-only** - Created only for system state tracking

**Key Files:**
- `packages/zero-cache/src/auth/read-authorizer.ts:32-34`
- `packages/zero-cache/src/services/view-syncer/cvr.ts:82-89, 1020-1049`
- `packages/zero-protocol/src/queries-patch.ts:5-18`

---

### ZSP-006: Default Permission Model ✅

**Status:** SECURE

Zero implements a secure deny-all default:

- **No rules = No access:** Tables without permission rules return empty results
- **authData resolves to NULL:** When undefined, auth references fail safely
- **Explicit allow required:** Must use `ANYONE_CAN` to enable public access
- **Subquery protection:** RLS applies to correlated subqueries (prevents oracle attacks)

**Key Files:**
- `packages/zero-cache/src/auth/read-authorizer.ts:68-84`
- `packages/zql/src/builder/builder.ts:201-215`

---

## Attack Surface Summary

### Entry Points

| Entry Point | Location | Risk |
|-------------|----------|------|
| WebSocket Upgrade | `websocket-handoff.ts:111-129` | Medium |
| Message Handler | `connection.ts:177-215` | Low |
| Custom Mutations | `syncer-ws-message-handler.ts:89-134` | Low (delegated to external API) |
| Custom Queries | `transform-query.ts:111-125` | Low (delegated to external API) |
| Admin Endpoints | `/statz`, `/heapz` | Medium (password required in prod) |

### Authentication Flow

```
Legacy Path:    Client → WebSocket → JWT Verification → Message Parsing → Permission System → Database
Custom Path:    Client → WebSocket → Token Forwarding → External API → Response Validation → Database
```

---

## Recommendations

### Immediate Actions (Critical)

1. **Fix /heapz auth bypass** - Add `return;` statement after 401 response in `heapz.ts:17`
2. **Secure replication endpoints** - Add authentication to `/replication/v*/changes` and `/replication/v*/snapshot`

### Short-term Actions (High)

3. **Add SSRF protections** - IP validation, localhost blocking, metadata blocking for custom endpoint URLs
4. **Add WebSocket message size limits** - Configure `maxPayload` on WebSocket server (recommend 1MB)
5. **Add connection rate limiting** - Per-IP and per-clientID throttling for WebSocket connections
6. **Sanitize error messages** - Never include raw exception messages in client responses
7. **Add shard-level authorization** - Validate clients can only access authorized shards
8. **Add zero-server authentication** - Implement API key or mTLS between zero-cache and zero-server

### Medium-term Actions

9. **Add Origin header validation** - Implement CORS for WebSocket upgrades (or document risk acceptance)
10. **Rate limit all message types** - Extend rate limiting beyond mutations to queries and other messages
11. **Encrypt SQLite replica** - Use encryption at rest for replica files
12. **Add request/response signing** - Cryptographic integrity for external endpoints (defense in depth)
13. **Security documentation** - Clear guidance on secure deployment with custom endpoints and zero-server
14. **Audit logging** - Log all custom endpoint requests and responses
15. **Document zero-server deployment** - Network isolation requirements, authentication best practices

---

## Penetration Testing Plan

### Phase 1: Legacy Mutation Testing
- [ ] Connect without token, attempt CRUD mutations → expect DENY
- [ ] Connect with invalid token, attempt CRUD mutations → expect DENY
- [ ] Connect with valid token but missing required claims → expect DENY
- [ ] Verify `lastMutationID` increments but no data changes on denied mutations

### Phase 2: Custom Mutation Testing
- [ ] Send custom mutation with no token → verify external API receives no auth
- [ ] Send custom mutation with forged token → verify external API rejects
- [ ] Attempt to bypass external API URL validation → expect block
- [ ] Verify 401/403 from external API properly propagates to client

### Phase 3: Mixed Mode Testing
- [ ] Configure custom endpoints without JWT verification
- [ ] Send CRUD mutation → verify Zero permission system blocks
- [ ] Send custom mutation → verify external API receives and validates
- [ ] Attempt to route CRUD mutation to custom endpoint → verify routing enforced

### Phase 4: Edge Cases
- [ ] Empty mutations array
- [ ] Mutation with invalid type field
- [ ] Mutation with mismatched name/type
- [ ] Concurrent CRUD and custom mutations (ordering issues)

### Phase 5: Legacy Query Testing
- [ ] Connect without token, query table with authData-based rules → expect empty results
- [ ] Connect with valid token, query table → expect RLS-filtered results
- [ ] Query with subquery → verify RLS applies to subquery (oracle prevention)
- [ ] Query table with no permission rules → expect empty results (default deny)

### Phase 6: Custom Query Testing
- [ ] Send custom query with no token → verify external API receives no auth
- [ ] Send custom query with forged token → verify external API rejects
- [ ] Verify returned AST is executed locally, not trusted blindly
- [ ] Test URL validation bypass attempts → expect rejection

### Phase 7: Query Routing Testing
- [ ] Send query with both ast AND name/args → expect assertion failure
- [ ] Attempt to use reserved internal query IDs (`lmids`, `mutationResults`) → expect rejection
- [ ] Test mixed mode: client query and custom query in same session

### Phase 8: WebSocket Handshake Security
- [ ] Connect from malicious origin → verify connection allowed (document behavior)
- [ ] Attempt connection without sec-websocket-protocol header → expect rejection
- [ ] Flood connection attempts from single IP → verify no rate limiting

### Phase 9: WebSocket Message Security
- [ ] Send oversized message (10MB+) → check for memory exhaustion
- [ ] Send malformed JSON → verify graceful error handling
- [ ] Flood with changeDesiredQueries messages → verify no rate limiting
- [ ] Check error messages for sensitive data leakage

### Phase 10: WebSocket Connection State
- [ ] Connect with same clientID as existing connection → verify old connection closed
- [ ] Connect without wsID → verify behavior with empty string wsID
- [ ] Attempt to send messages with wrong wsID → expect silent drop
- [ ] Test keepalive timeout (stop responding to pings)

### Phase 11: Admin Endpoints (CRITICAL)
- [ ] Access /heapz without credentials → expect heap snapshot received (CONFIRM BUG)
- [ ] Access /statz without credentials → expect 401 only (correct behavior)
- [ ] Verify admin password is required in production mode

### Phase 12: Replication Security (CRITICAL)
- [ ] Connect to /replication/v*/changes without auth → expect change stream (CONFIRM BUG)
- [ ] Connect to /replication/v*/snapshot without auth → expect snapshot (CONFIRM BUG)
- [ ] Subscribe to wrong shard → verify no authorization check
- [ ] Verify replication ports are not exposed publicly

### Phase 13: Zero-Server Security
- [ ] Send mutation request directly to zero-server (bypassing zero-cache) → verify accepted without auth
- [ ] Attempt SQL injection in mutation args → verify parameterized queries block
- [ ] Send mutation with duplicate ID → verify idempotency rejects duplicate
- [ ] Send out-of-order mutation IDs → verify ordering enforcement
- [ ] Test prototype pollution in mutator name lookup → expect safe handling
- [ ] Send malformed PushRequest → verify schema validation rejects
- [ ] Check error responses for sensitive information leakage
- [ ] Test concurrent mutations to same row → verify transaction isolation

---

## Appendix: Files Reviewed

### Critical Security Files

| Package | File | Purpose |
|---------|------|---------|
| zero-cache | `src/auth/read-authorizer.ts` | Read permission enforcement |
| zero-cache | `src/auth/write-authorizer.ts` | Write permission enforcement (default deny at line 516-521) |
| zero-cache | `src/workers/syncer.ts` | Connection authentication (line 143-193) |
| zero-cache | `src/workers/syncer-ws-message-handler.ts` | Mutation routing (line 121-134) |
| zero-cache | `src/auth/jwt.ts` | Token verification |
| zero-cache | `src/config/zero-config.ts` | Admin password validation |
| zero-cache | `src/services/mutagen/mutagen.ts` | Legacy mutation processing (line 336-340) |
| zero-cache | `src/services/mutagen/pusher.ts` | Custom mutation forwarding (line 110-124, 452-500) |
| zero-cache | `src/custom/fetch.ts` | URL validation for custom endpoints (line 77-95, 248-258) |
| zero-cache | `src/custom-queries/transform-query.ts` | Custom query transformation (line 74-125) |
| zero-cache | `src/services/view-syncer/cvr.ts` | Query routing and internal query protection (line 82-89, 1020-1050) |
| zero-cache | `src/services/view-syncer/view-syncer.ts` | Query processing orchestration, wsID validation (line 870-876) |
| zero-cache | `src/server/worker-dispatcher.ts` | WebSocket routing |
| zero-cache | `src/workers/connect-params.ts` | Connection parameter extraction (line 41-47) |
| zero-cache | `src/services/limiter/sliding-window-limiter.ts` | Mutation rate limiting |
| zero-cache | `src/services/heapz.ts` | Heap snapshot endpoint (VULNERABLE - line 16-21) |
| zero-cache | `src/services/statz.ts` | Statistics endpoint (secure) |
| zero-cache | `src/services/change-streamer/change-streamer-http.ts` | Replication endpoints (VULNERABLE) |
| zero-cache | `src/config/normalize.ts` | Production config validation |
| zqlite | `src/internal/sql.ts` | SQL parameterization |
| zqlite | `src/query-builder.ts` | Query construction |
| zero-protocol | `src/ast.ts` | AST validation schemas |
| zero-server | `src/process-mutations.ts` | Mutation entry point |
| zero-server | `src/push-processor.ts` | Public mutation API |
| zero-server | `src/queries/process-queries.ts` | Query transformation |
| zero-server | `src/custom.ts` | CRUD operations with safe SQL |
| z2s | `src/sql.ts` | Parameterized SQL generation |
| shared | `src/object-traversal.ts` | Prototype pollution prevention |
| zql | `src/query/query-registry.ts` | Query lookup via mustGetQuery() |

### Test Coverage

- `read-authorizer.test.ts` - Permission transformation tests
- `write-authorizer.test.ts` - Mutation authorization tests
- `is-admin-password-valid.test.ts` - Admin bypass confirmation
- `fetch.test.ts` - URL validation tests

---

**Report Classification:** Internal Use Only
**Distribution:** Development Team, Security Team
**Next Review:** After remediation of critical findings
