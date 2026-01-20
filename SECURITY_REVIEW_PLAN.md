# Zero Cache & Zero Server Security Review Plan

## Executive Summary

This plan outlines a comprehensive security review for `zero-cache` and `zero-server` packages. Based on initial analysis, the codebase has existing pentest coverage but several areas require deeper investigation.

---

## 1. Authentication & Authorization

### 1.1 JWT Implementation (zero-cache)
**Files**: `packages/zero-cache/src/auth/jwt.ts`
**Risk**: High

- [ ] Review JWT validation logic for algorithm confusion attacks
- [ ] Verify JWK/JWKS URL fetching doesn't allow SSRF
- [ ] Test symmetric secret handling (deprecated but present)
- [ ] Verify token expiration is enforced post-connection
- [ ] Review claim validation (subject, iat, nbf, exp)

### 1.2 Write Authorization (zero-cache)
**Files**: `packages/zero-cache/src/auth/write-authorizer.ts`
**Risk**: High

- [ ] Review pre-mutation vs post-mutation permission checks
- [ ] Verify table/column/row/cell level policies work correctly
- [ ] Test permission bypass via edge cases (null values, empty arrays)
- [ ] Review policy evaluation against SQLite replica

### 1.3 Read Authorization (zero-cache)
**Files**: `packages/zero-cache/src/auth/read-authorizer.ts`
**Risk**: High

- [ ] Verify AST transformation adds correct WHERE clauses
- [ ] Test for existence oracle attacks via subqueries
- [ ] Review default-deny behavior when no permissions defined
- [ ] Test permission bypass via complex joins/subqueries

### 1.4 Permission Loading (zero-cache)
**Files**: `packages/zero-cache/src/auth/load-permissions.ts`
**Risk**: Medium

- [ ] Review hash-based permission change detection
- [ ] Test permission table poisoning via replication
- [ ] Verify permission schema validation

### 1.5 Application-Level Auth (zero-server)
**Files**: `packages/zero-server/src/push-processor.ts`
**Risk**: Critical - No built-in auth

- [ ] Document trust boundary assumptions
- [ ] Review context parameter usage in mutators
- [ ] Verify no auth bypass via malformed requests

---

## 2. Input Validation & Injection

### 2.1 SQL Injection
**Files**:
- `packages/zero-cache/src/db/queries.ts`, `statements.ts`
- `packages/zero-server/src/custom.ts`
**Risk**: High

- [ ] Verify all queries use parameterized statements
- [ ] Test SQL injection in table/column names
- [ ] Review dynamic query construction paths
- [ ] Test PostgreSQL-specific injection (COPY, CREATE EXTENSION)
- [ ] Verify `@databases/sql` template usage

### 2.2 JSON Parsing DoS
**Files**: `packages/zero-cache/src/workers/syncer-ws-message-handler.ts`
**Risk**: Medium

- [ ] Test deeply nested JSON (current finding: accepts 5000 levels)
- [ ] Test large JSON payloads for memory exhaustion
- [ ] Review `NODE_ENV=production` assertion bypass
- [ ] Test JSON parsing timeout behavior

### 2.3 Prototype Pollution
**Files**: `packages/zero-server/src/push-processor.ts`
**Risk**: Medium (already tested)

- [ ] Verify mutator path resolution is safe
- [ ] Test `__proto__`, `constructor.prototype` in mutation names
- [ ] Review `getValueAtPath()` implementation

### 2.4 WebSocket Protocol Parsing
**Files**: `packages/zero-cache/src/workers/connection.ts`
**Risk**: Medium

- [ ] Test malformed protocol version strings
- [ ] Test oversized base64 tokens in sec-websocket-protocol
- [ ] Review protocol upgrade handling

---

## 3. Database Security

### 3.1 Connection Pool Management
**Files**: `packages/zero-cache/src/config/zero-config.ts`
**Risk**: Medium

- [ ] Review max connection limits
- [ ] Test connection exhaustion attacks
- [ ] Verify connection string handling (credentials in memory)

### 3.2 Replication Security (zero-cache)
**Files**: `packages/zero-cache/src/services/change-streamer/`
**Risk**: High

- [ ] Review logical replication slot security
- [ ] Test for replication data poisoning
- [ ] Verify publication filtering
- [ ] Review multi-worker sync mechanism

### 3.3 SQLite Replica Security
**Files**: `packages/zero-cache/src/db/lite-tables.ts`
**Risk**: Medium

- [ ] Review file permissions for replica DB
- [ ] Test backup/restore integrity (Litestream)
- [ ] Verify vacuum operation safety

### 3.4 Transaction Isolation (zero-server)
**Files**: `packages/zero-server/src/process-mutations.ts`
**Risk**: Medium

- [ ] Review transaction boundary handling
- [ ] Test rollback behavior on errors
- [ ] Verify LMID tracking atomicity

---

## 4. Network & Protocol Security

### 4.1 WebSocket Security
**Files**:
- `packages/zero-cache/src/workers/syncer.ts`
- `packages/zero-cache/src/types/websocket-handoff.ts`
**Risk**: High

- [ ] Review WebSocket handoff between processes
- [ ] Test connection hijacking scenarios
- [ ] Verify keepalive/pong mechanism
- [ ] Test connection state machine for invalid transitions

### 4.2 Custom Endpoint SSRF
**Files**: `packages/zero-cache/src/custom/fetch.ts`
**Risk**: High

- [ ] Review URLPattern validation
- [ ] Test SSRF via overly permissive patterns
- [ ] Test URL parameter injection (schema, appID)
- [ ] Review header injection possibilities
- [ ] Test redirect following behavior

### 4.3 HTTP Header Handling
**Files**: `packages/zero-cache/src/types/http.ts`
**Risk**: Medium

- [ ] Review Authorization header extraction
- [ ] Test header injection via base64 encoding
- [ ] Verify no CRLF injection in headers

---

## 5. Configuration & Secrets

### 5.1 Environment Variable Handling
**Files**: `packages/zero-cache/src/config/zero-config.ts`, `normalize.ts`
**Risk**: Medium

- [ ] Review credential handling (ZERO_UPSTREAM_DB connection strings)
- [ ] Verify secrets not logged
- [ ] Test configuration validation bypass
- [ ] Review admin password handling

### 5.2 Development vs Production
**Files**: Various
**Risk**: Medium

- [ ] Document all production vs development behavior differences
- [ ] Test for debug endpoints in production
- [ ] Verify admin panel security (ZERO_ADMIN_PASSWORD)

---

## 6. Error Handling & Information Disclosure

### 6.1 Error Message Leakage
**Files**:
- `packages/zero-cache/src/types/error.ts`
- `packages/zero-server/src/process-mutations.ts`
**Risk**: Medium

- [ ] Review error messages for SQL/system info leakage
- [ ] Test error details field content
- [ ] Verify stack traces not exposed to clients

### 6.2 Timing Attacks
**Files**: Various auth files
**Risk**: Low-Medium

- [ ] Review JWT validation for timing differences
- [ ] Test permission check timing leakage
- [ ] Review password comparison (admin panel)

---

## 7. Business Logic

### 7.1 Mutation Replay Prevention
**Files**: `packages/zero-server/src/process-mutations.ts`
**Risk**: High

- [ ] Review LMID tracking mechanism
- [ ] Test race conditions in mutation processing
- [ ] Verify out-of-order mutation handling
- [ ] Test boundary conditions (MAX_SAFE_INTEGER, negative IDs)

### 7.2 Client State Management
**Files**:
- `packages/zero-cache/src/services/view-syncer/view-syncer.ts`
- `packages/zero-cache/src/services/view-syncer/cvr.ts`
**Risk**: Medium

- [ ] Review client view record integrity
- [ ] Test for state confusion attacks
- [ ] Verify version cookie handling

### 7.3 Query Processing
**Files**:
- `packages/zero-cache/src/services/view-syncer/pipeline-driver.ts`
- `packages/zero-server/src/queries/process-queries.ts`
**Risk**: Medium

- [ ] Review ZQL to SQL transformation security
- [ ] Test complex query resource exhaustion
- [ ] Verify name mapping (clientToServer) is safe

---

## 8. External Integrations

### 8.1 PostgreSQL Integration
**Risk**: High

- [ ] Review all SQL generation paths
- [ ] Test for privilege escalation via SQL
- [ ] Verify publication/subscription security

### 8.2 Litestream Backup
**Files**: `packages/zero-cache/src/services/litestream/`
**Risk**: Medium

- [ ] Review backup location validation
- [ ] Test restore from malicious backup
- [ ] Verify encryption settings

### 8.3 OpenTelemetry
**Files**: Various OTEL integration points
**Risk**: Low

- [ ] Review what data is sent to OTEL
- [ ] Verify no PII in traces/metrics

---

## 9. Known Issues from Existing Pentests

### Already Tested (Low Priority)
- JWT malformation, expiry, alg:none attacks - **PASS**
- RLS bypass attempts - **PASS**
- SQL injection via ZQL - **PASS**
- Prototype pollution in mutators - **PASS**
- Query fuzzing - **PASS**

### Documented Findings Requiring Follow-up
1. **Production JSON Validation Bypass**: `NODE_ENV=production` disables depth assertions
2. **No JSON Depth Limit**: Accepts 5000+ nesting levels
3. **Missing Base64 Size Limits**: On sec-websocket-protocol header
4. **Unicode Homoglyph Confusion**: No NFC normalization on identifiers
5. **Unhandled Auth Error**: Token sent without JWT config throws unhandled error

---

## Priority Matrix

| Area | Risk | Effort | Priority |
|------|------|--------|----------|
| JWT/Auth Implementation | High | Medium | P0 |
| Read/Write Authorization | High | High | P0 |
| SSRF in Custom Endpoints | High | Medium | P0 |
| SQL Injection | High | Medium | P0 |
| Replication Security | High | High | P1 |
| WebSocket Security | High | Medium | P1 |
| Mutation Replay Prevention | High | Medium | P1 |
| JSON Parsing DoS | Medium | Low | P2 |
| Error Information Disclosure | Medium | Low | P2 |
| Configuration Security | Medium | Low | P2 |

---

## Recommended Review Order

1. **Phase 1 - Critical Auth/Authz** (P0)
   - JWT implementation
   - Write authorizer
   - Read authorizer
   - Custom endpoint SSRF

2. **Phase 2 - Data Security** (P1)
   - SQL injection comprehensive review
   - Replication security
   - WebSocket protocol security
   - Mutation LMID tracking

3. **Phase 3 - Defense in Depth** (P2)
   - JSON parsing limits
   - Error handling
   - Configuration validation
   - Unicode normalization

---

## Test Environment Requirements

- PostgreSQL 15+ with logical replication enabled
- SQLite for replica testing
- Network isolation for SSRF testing
- Multiple concurrent client simulation
- Fuzzing infrastructure for protocol testing

---

## Deliverables

1. Detailed findings report with severity ratings
2. Proof-of-concept exploits for any vulnerabilities
3. Remediation recommendations
4. Regression test cases for identified issues
