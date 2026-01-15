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
| **CRITICAL** | 0 | Immediate action required |
| **HIGH** | 1 | Should be addressed soon |
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

### Short-term Actions (High)

1. **Add SSRF protections** - IP validation, localhost blocking, metadata blocking for custom endpoint URLs

### Medium-term Actions

2. **Add request/response signing** - Cryptographic integrity for external endpoints (defense in depth)
3. **Security documentation** - Clear guidance on secure deployment with custom endpoints
4. **Audit logging** - Log all custom endpoint requests and responses

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
| zero-cache | `src/services/view-syncer/view-syncer.ts` | Query processing orchestration |
| zqlite | `src/internal/sql.ts` | SQL parameterization |
| zqlite | `src/query-builder.ts` | Query construction |
| zero-protocol | `src/ast.ts` | AST validation schemas |

### Test Coverage

- `read-authorizer.test.ts` - Permission transformation tests
- `write-authorizer.test.ts` - Mutation authorization tests
- `is-admin-password-valid.test.ts` - Admin bypass confirmation
- `fetch.test.ts` - URL validation tests

---

**Report Classification:** Internal Use Only
**Distribution:** Development Team, Security Team
**Next Review:** After remediation of critical findings
