# Security Review Phase 1.5 - P1 Priority Findings

## Executive Summary

This document contains the security review findings for Phase 1.5 (P1 priority items):
- **3.2 Replication Security** (zero-cache)
- **4.1 WebSocket Security** (zero-cache)
- **7.1 Mutation Replay Prevention** (zero-server)

**Overall Assessment**: 2 critical vulnerabilities, 3 high-severity issues, and several medium/low issues identified.

---

## Critical Findings

### CRITICAL-01: SQL Injection in Replication Stream Initialization

**Severity**: CRITICAL
**CVSS Score**: 8.1 (High)
**File**: `packages/zero-cache/src/services/change-source/pg/logical-replication/stream.ts`
**Lines**: 137-143

**Vulnerability Description**:

The `publications` array is interpolated directly into the SQL string using template literals without proper escaping:

```typescript
const stream = session
  .unsafe(
    `START_REPLICATION SLOT "${slot}" LOGICAL ${fromBigInt(lsn)} (
    proto_version '1',
    publication_names '${publications}',  // <-- VULNERABLE
    messages 'true'
  )`,
  )
  .execute();
```

**Attack Vector**:
- An attacker who can influence the `publications` array (e.g., via configuration poisoning or upstream database modification) can inject arbitrary PostgreSQL replication protocol commands.
- Example payload: `pub', 'injected_pub` breaks out of the string context.

**Impact**:
- Potential command injection into PostgreSQL replication protocol
- Data exfiltration via malicious publication names
- Denial of service via malformed replication commands

**Root Cause**:
- Publications come from `shardConfig` table in upstream database (shard.ts:252-265)
- Validation only checks that publications exist in `pg_publication`, not that names are safe for interpolation (shard.ts:276-300)

**Recommendation**:
1. Use parameterized queries or proper array escaping for publications
2. Add strict validation for publication names (alphanumeric only, length limits)
3. Consider using `pg-format` library's `literal()` function for safe interpolation

---

### CRITICAL-02: Connection Hijacking via Missing Auth Re-validation

**Severity**: HIGH-CRITICAL
**CVSS Score**: 7.5
**File**: `packages/zero-cache/src/workers/syncer.ts`
**Lines**: 134-140

**Vulnerability Description**:

When a new connection arrives with a `clientID` that already has an existing connection, the existing connection is closed without verifying that the new connection has equivalent or higher authorization:

```typescript
const existing = this.#connections.get(clientID);
if (existing) {
  this.#lc.debug?.(
    `client ${clientID} already connected, closing existing connection`,
  );
  existing.close(`replaced by ${params.wsID}`);  // No auth comparison!
}
```

**Attack Scenario**:
1. Client A (user_id=alice, clientID=X) establishes authenticated connection
2. Attacker (user_id=bob, clientID=X) sends connection request with different token
3. Client A's connection is forcibly closed by attacker
4. Attacker gains denial-of-service over specific clients

**Impact**:
- Targeted denial-of-service against authenticated users
- Potential privilege confusion if clientIDs are not properly scoped to users
- Session hijacking if combined with other vulnerabilities

**Recommendation**:
1. Verify new connection's auth token matches or supersedes existing connection's user
2. Add rate limiting on connection replacements per clientID
3. Log and alert on suspicious connection replacement patterns
4. Consider requiring cryptographic proof of clientID ownership

---

## High Severity Findings

### HIGH-01: Auth Token Validation Bypass When Config Empty

**Severity**: HIGH
**File**: `packages/zero-cache/src/workers/syncer.ts`
**Lines**: 164-190

**Vulnerability Description**:

When `tokenOptions.length === 0` and custom endpoints are configured, the auth token is NOT verified - only a warning is logged:

```typescript
if (tokenOptions.length > 0) {
  // Token is verified
} else {
  lc.warn?.(
    `One of jwk, secret, or jwksUrl is not configured - ` +
    `the authorization header must be manually verified by the user`,
  );
  // Connection proceeds WITHOUT verification!
}
```

**Impact**:
- Complete authentication bypass if JWT config is misconfigured
- Silent failure - only a warning log, no connection rejection

**Recommendation**:
1. Fail closed: reject connections when auth is provided but cannot be verified
2. Add explicit `ZERO_AUTH_REQUIRED=true` config option that enforces validation
3. Upgrade warning to error and reject connection

---

### HIGH-02: Unbounded Caches for Replication Type/Relation Metadata

**Severity**: HIGH
**File**: `packages/zero-cache/src/services/change-source/pg/logical-replication/pgoutput-parser.ts`
**Lines**: 29-30, 98, 126

**Vulnerability Description**:

The `PgoutputParser` maintains unbounded caches for type and relation metadata:

```typescript
readonly #typeCache = new Map<number, Parser>();  // Line 29
readonly #relationCache = new Map<number, MessageRelation>();  // Line 30
```

These caches grow indefinitely as new types/relations are encountered through replication.

**Impact**:
- Memory exhaustion attack via crafted replication stream with many unique type OIDs
- Long-running processes accumulate memory over time
- Potential denial of service

**Recommendation**:
1. Implement LRU cache with bounded size for both caches
2. Add periodic cache cleanup or size monitoring
3. Log warnings when cache exceeds threshold sizes

---

### HIGH-03: Automatic REPLICATION Role Grant

**Severity**: MEDIUM-HIGH
**File**: `packages/zero-cache/src/services/change-source/pg/initial-sync.ts`
**Lines**: 94-107

**Vulnerability Description**:

When replication slot creation fails due to insufficient privileges, the code automatically grants the REPLICATION role:

```typescript
if (e.code === PG_INSUFFICIENT_PRIVILEGE) {
  await sql`ALTER ROLE current_user WITH REPLICATION`;
  lc.info?.(`Added the REPLICATION role to database user`);
  continue;
}
```

**Impact**:
- Privilege escalation without explicit authorization
- May violate principle of least privilege
- No audit trail of who authorized the privilege grant

**Recommendation**:
1. Remove automatic privilege escalation
2. Fail with clear error message about required privileges
3. Document required privileges in deployment guide
4. Add configuration flag to explicitly allow auto-grant

---

## Medium Severity Findings

### MEDIUM-01: Missing Socket State Validation During Handoff

**File**: `packages/zero-cache/src/types/websocket-handoff.ts`
**Lines**: 137-146

**Description**: Socket existence is checked but not writability/validity state before handleUpgrade.

**Recommendation**: Add socket state validation (writable, not destroyed) before upgrade.

---

### MEDIUM-02: No Timeout on initConnectionMsg Processing

**File**: `packages/zero-cache/src/workers/syncer.ts`
**Lines**: 240-249

**Description**: `handleInitConnection()` has no timeout, allowing slow processing to block.

**Recommendation**: Add 5-10 second timeout on initConnectionMsg processing.

---

### MEDIUM-03: ClientGroupID Validation Occurs After Connection Creation

**File**: `packages/zero-cache/src/workers/syncer-ws-message-handler.ts`
**Lines**: 94-108

**Description**: ClientGroupID in messages is validated after connection is established, not during handshake.

**Recommendation**: Validate clientGroupID during connection setup for fail-fast behavior.

---

### MEDIUM-04: No Upper Bound on Cleanup upToMutationID

**File**: `packages/zero-protocol/src/push.ts`
**Lines**: 20-24

**Description**: The cleanup mutation accepts any `upToMutationID` value without validation.

**Recommendation**: Validate that upToMutationID is less than or equal to current lastMutationID.

---

### MEDIUM-05: Type Coercion in LMID Comparison

**File**: `packages/zero-server/src/process-mutations.ts`
**Lines**: 509-523

**Description**: Comparison between `number` (receivedMutationID) and `bigint` (lastMutationID) could lose precision for values > 2^53-1.

**Recommendation**: Add explicit type handling or use BigInt consistently.

---

## Low Severity Findings

### LOW-01: Non-Integer Mutation IDs Not Validated

**File**: `packages/zero-protocol/src/push.ts`

**Description**: Schema allows floating-point numbers for mutation IDs (e.g., 1.5).

**Recommendation**: Add `.integer()` validation to mutation ID schema.

---

### LOW-02: Cleanup Errors Swallowed Silently

**File**: `packages/zero-server/src/process-mutations.ts`
**Lines**: 266-273

**Description**: Cleanup mutation errors are logged but don't fail the push.

**Recommendation**: Document this is intentional or consider adding monitoring.

---

## Security Strengths Observed

### Mutation Replay Prevention (STRONG)

The LMID tracking mechanism is well-designed:

1. **Atomic LMID Increment**: PostgreSQL `ON CONFLICT ... DO UPDATE` is atomic
2. **SERIALIZABLE Transactions**: Prevents race conditions
3. **Replay Attack Prevention**: Mutations with lower IDs silently dropped
4. **Out-of-Order Detection**: Mutations with higher IDs rejected with clear error
5. **Error Persistence**: Application errors stored for client visibility

### WebSocket Connection Management (GOOD)

Several positive patterns:

1. **Idempotent Close**: `if (this.#closed) return` prevents double-cleanup
2. **Post-Close Message Guard**: Messages after close are discarded
3. **Protocol Version Validation**: Invalid versions rejected at init
4. **Connection Identity Verification**: Close callback verifies connection identity
5. **Backpressure-Aware Keepalive**: Pongs sent even during command backpressure

### Replication Data Parsing (GOOD)

1. **Strict Message Tag Validation**: Single byte enum enforced
2. **Typed Constructors**: All parsing goes through typed constructors
3. **Binary Format Control**: Postgres-controlled, not user-influenced

---

## Remediation Priority

| Finding | Severity | Effort | Priority |
|---------|----------|--------|----------|
| CRITICAL-01: SQL Injection in publications | Critical | Low | P0 - Immediate |
| CRITICAL-02: Connection hijacking | High-Critical | Medium | P0 - Immediate |
| HIGH-01: Auth validation bypass | High | Low | P0 - This Sprint |
| HIGH-02: Unbounded caches | High | Medium | P1 - Next Sprint |
| HIGH-03: Auto role grant | Medium-High | Low | P1 - Next Sprint |
| MEDIUM-01 through MEDIUM-05 | Medium | Low-Medium | P2 |
| LOW-01 through LOW-02 | Low | Low | P3 |

---

## Test Cases to Add

### For CRITICAL-01 (SQL Injection):
```typescript
test('rejects publication names with SQL injection characters', async () => {
  const malicious = ["pub', 'injected"];
  await expect(subscribe(lc, db, slot, malicious, lsn)).rejects.toThrow();
});
```

### For CRITICAL-02 (Connection Hijacking):
```typescript
test('validates auth token matches when replacing connection', async () => {
  // Create connection with user A's token
  // Attempt replacement with user B's token
  // Expect rejection, not replacement
});
```

### For Mutation Replay:
```typescript
test('handles mutation ID at Number.MAX_SAFE_INTEGER boundary', async () => {
  // Test with ID = 2^53-1
  // Verify no precision loss
});
```

---

## Files Requiring Security Review Follow-up

1. `packages/zero-cache/src/services/change-source/pg/logical-replication/stream.ts` - SQL injection fix
2. `packages/zero-cache/src/workers/syncer.ts` - Auth validation improvements
3. `packages/zero-cache/src/services/change-source/pg/schema/shard.ts` - Publication validation
4. `packages/zero-cache/src/services/change-source/pg/logical-replication/pgoutput-parser.ts` - Cache bounds
5. `packages/zero-server/src/process-mutations.ts` - Type handling for large mutation IDs

---

## Appendix: Code References

### Replication Stream Start
- `packages/zero-cache/src/services/change-source/pg/logical-replication/stream.ts:137-143`

### Connection Creation
- `packages/zero-cache/src/workers/syncer.ts:126-250`

### LMID Tracking
- `packages/zero-server/src/process-mutations.ts:504-524`
- `packages/zero-cache/src/services/mutagen/mutagen.ts:431-461`

### WebSocket Handoff
- `packages/zero-cache/src/types/websocket-handoff.ts:131-155`

### JWT Verification
- `packages/zero-cache/src/auth/jwt.ts:49-89`
