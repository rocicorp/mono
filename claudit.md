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
| **HIGH** | 1 | Should be addressed soon |
| **LOW** | 0 | Acceptable risk |

### Key Findings

| ID | Finding | Severity | Status |
|----|---------|----------|--------|
| ZSP-001 | Development Mode Admin Bypass | **CRITICAL** | Open |
| ZSP-002 | Custom Endpoint JWT Verification Skip | **CRITICAL** | Open |
| ZSP-003 | Weak SSRF Protection | **HIGH** | Open |
| ZSP-004 | SQL Injection Protections | ✅ Secure | N/A |
| ZSP-005 | `internalQuery` RLS Bypass | ✅ Secure | N/A |
| ZSP-006 | Default Permission Model | ✅ Secure | N/A |

---

## Detailed Findings

### ZSP-001: Development Mode Admin Bypass

**Severity:** CRITICAL
**CVSS Score:** 9.1 (Critical)
**CWE:** CWE-287 (Improper Authentication)

#### Description

When the environment variable `NODE_ENV` is set to `development`, the Zero Cache server allows access to administrative endpoints without password authentication. This exposes sensitive debugging interfaces that can leak credentials and application data.

#### Affected Components

| File | Lines | Function |
|------|-------|----------|
| `packages/zero-cache/src/config/normalize.ts` | 27-29 | `isDevelopmentMode()` |
| `packages/zero-cache/src/config/zero-config.ts` | 885-916 | `isAdminPasswordValid()` |
| `packages/zero-cache/src/services/statz.ts` | 304-329 | `handleStatzRequest()` |
| `packages/zero-cache/src/services/heapz.ts` | 9-38 | `handleHeapzRequest()` |
| `packages/zero-cache/src/server/inspector-delegate.ts` | 111-115 | `isAuthenticated()` |

#### Vulnerable Code

```typescript
// packages/zero-cache/src/config/zero-config.ts:885-916
export function isAdminPasswordValid(
  lc: LogContext,
  config: Pick<NormalizedZeroConfig, 'adminPassword'>,
  password: string | undefined,
) {
  if (!password && !config.adminPassword && isDevelopmentMode()) {
    warnOnce(lc, 'No admin password set; allowing access in development mode only');
    return true;  // BYPASS - no authentication required
  }
  // ...
}
```

#### Exposed Endpoints

1. **`/statz`** - Database statistics, client metadata, query ASTs
2. **`/heapz`** - V8 heap snapshots containing all in-memory data (credentials, tokens, user data)
3. **Inspector Protocol** - Query analysis, permission inspection, metrics

#### Attack Vectors

- Environment variable injection via Docker/Kubernetes configuration
- CI/CD pipeline misconfiguration
- Cloud deployment platform settings
- Local `.env` files committed to version control

#### Impact

- **Confidentiality:** Complete exposure of database schema, query patterns, and in-memory credentials
- **Integrity:** Attackers can analyze permission model to find bypass opportunities
- **Availability:** Repeated `/heapz` requests could cause memory exhaustion

#### Proof of Concept

```bash
# If NODE_ENV=development is set:
curl http://zero-cache:4848/statz  # Returns full database stats
curl http://zero-cache:4848/heapz  # Downloads V8 heap snapshot
```

#### Remediation

1. **Short-term:** Add explicit admin password requirement regardless of NODE_ENV
2. **Long-term:** Replace NODE_ENV checks with dedicated security configuration flag
3. **Defense-in-depth:** Rate limit admin endpoints, add IP allowlisting

---

### ZSP-002: Custom Endpoint JWT Verification Skip

**Severity:** CRITICAL
**CVSS Score:** 8.6 (High)
**CWE:** CWE-306 (Missing Authentication for Critical Function)

#### Description

When custom mutation/query URLs are configured (`ZERO_MUTATE_URL` + `ZERO_QUERY_URL`) without JWT verification options (`jwk`, `secret`, or `jwksUrl`), the Zero Cache server completely skips JWT validation and forwards raw authentication tokens to external servers without any verification.

#### Affected Components

| File | Lines | Function |
|------|-------|----------|
| `packages/zero-cache/src/workers/syncer.ts` | 153-190 | `#createConnection()` |
| `packages/zero-cache/src/services/mutagen/pusher.ts` | 110-124 | `enqueuePush()` |
| `packages/zero-cache/src/custom/fetch.ts` | 96-108 | Header construction |

#### Vulnerable Code

```typescript
// packages/zero-cache/src/workers/syncer.ts:153-190
const hasExactlyOneTokenOption = tokenOptions.length === 1;
const hasCustomEndpoints = hasPushOrMutate && hasQueries;

if (!hasExactlyOneTokenOption && !hasCustomEndpoints) {
  throw new Error('Exactly one of jwk, secret, or jwksUrl must be set...');
}

if (tokenOptions.length > 0) {
  decodedToken = await verifyToken(this.#config.auth, auth, {...});
} else {
  // JWT VERIFICATION COMPLETELY SKIPPED
  this.#lc.warn?.('One of jwk, secret, or jwksUrl is not configured...');
}
```

#### Attack Scenarios

1. **Token Replay:** Attacker intercepts valid JWT, uses it after expiration
2. **Forged Tokens:** Attacker creates arbitrary JWT claims without signature verification
3. **Bypass Zero Cache:** Attacker discovers external endpoint URL, sends direct requests

#### Impact

- **Authentication Bypass:** Invalid, expired, or forged tokens accepted
- **Authorization Bypass:** Attacker can assume any identity by forging JWT claims
- **Data Exposure:** Complete access to mutation/query functionality

#### Remediation

1. **Always verify JWTs at Zero Cache layer** even when custom endpoints are configured
2. **Add request signing:** Sign forwarded requests with a secret only Zero Cache knows
3. **Add response verification:** Require external servers to sign responses
4. **Documentation:** Clearly warn users about security implications

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
| Custom Mutations | `syncer-ws-message-handler.ts:89-134` | **Critical** |
| Custom Queries | `transform-query.ts:111-125` | **Critical** |
| Admin Endpoints | `/statz`, `/heapz` | **Critical** |

### Authentication Flow

```
Client → WebSocket → JWT Verification* → Message Parsing → Authorization → Database
                          ↓
                   * SKIPPED if custom endpoints configured without token options
```

---

## Recommendations

### Immediate Actions (Critical)

1. **Remove NODE_ENV security gate** - Use dedicated, non-standard env var
2. **Require admin password always** - Even in development environments
3. **Always verify JWTs** - Never skip verification regardless of configuration

### Short-term Actions (High)

4. **Add SSRF protections** - IP validation, localhost blocking, metadata blocking
5. **Add request/response signing** - Cryptographic integrity for external endpoints
6. **Rate limit admin endpoints** - Prevent DoS via repeated heap dumps

### Long-term Actions (Medium)

7. **Security documentation** - Clear guidance on secure deployment
8. **Security headers** - Add standard HTTP security headers
9. **Audit logging** - Log all admin endpoint access
10. **Penetration testing** - Active testing of identified vulnerabilities

---

## Appendix: Files Reviewed

### Critical Security Files

| Package | File | Purpose |
|---------|------|---------|
| zero-cache | `src/auth/read-authorizer.ts` | Read permission enforcement |
| zero-cache | `src/auth/write-authorizer.ts` | Write permission enforcement |
| zero-cache | `src/workers/syncer.ts` | Connection authentication |
| zero-cache | `src/auth/jwt.ts` | Token verification |
| zero-cache | `src/config/zero-config.ts` | Admin password validation |
| zero-cache | `src/services/mutagen/mutagen.ts` | Mutation processing |
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
