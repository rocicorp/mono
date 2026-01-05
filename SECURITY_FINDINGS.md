# Security Review Findings: Custom Mutator Pipeline

**Date**: 2026-01-05
**Scope**: Custom mutation pipeline (Client → Zero-Cache → API Server → PostgreSQL)
**Threat Model**: Unauthenticated attacker + Authenticated malicious user

---

## Executive Summary

| Severity | Count | Status |
|----------|-------|--------|
| **High** | 2 | ✅ FIXED |
| **Medium** | 4 | Open |
| **Low** | 3 | Open |

---

## High Severity Findings

### H1: Information Disclosure in `issue.update` Mutator ✅ FIXED

**File**: `apps/zbugs/shared/mutators.ts:99-115`

**Issue**: The mutator read the issue from the database BEFORE performing authorization checks. An attacker could determine if an issue exists (even private ones) by observing different error messages.

**Fix Applied**:
1. Moved `assertIsCreatorOrAdmin()` check BEFORE the existence check
2. Changed error message to generic "Issue not found or not authorized"
3. Also fixed `assertIsCreatorOrAdmin()` in `auth.ts` to use generic error messages

---

### H2: Missing Visibility Check in `viewState.set` Mutator ✅ FIXED

**File**: `apps/zbugs/shared/mutators.ts:319-333`

**Issue**: Any authenticated user could set view state for ANY issue, including private/internal issues they cannot access.

**Fix Applied**: Added `assertUserCanSeeIssue(tx, userID, issueID)` check before the upsert operation.

---

## Medium Severity Findings

### M1: Prototype Chain Access in Mutator Lookup

**File**: `packages/shared/src/object-traversal.ts:38`

**Issue**: The `in` operator checks the prototype chain, not just own properties. While `isMutator()` provides some protection, prototype methods can still be invoked as legacy mutators.

```typescript
if (current && typeof current === 'object' && part in current) {
  current = (current as Record<string, unknown>)[part];
}
```

**Attack Vector**: Mutation name `toString`, `valueOf`, or `constructor` will resolve to Object.prototype methods.

**Impact**: Low practical impact because:
- `isMutator()` check prevents new-style mutator invocation
- Legacy mutators just get called with `(dbTx, args)`, causing no-op or type errors

**Recommendation**: Use `Object.hasOwn(current, part)` instead of `part in current`.

---

### M2: JSON Validation Bypassed in Production

**File**: `packages/shared/src/json-schema.ts:12-13` + `packages/shared/src/config.ts:7-9`

**Issue**: When `NODE_ENV=production`, JSON type validation is completely skipped.

```typescript
// config.ts
export const isProd = process.env.NODE_ENV === 'production';
export {isProd as skipAssertJSONValue};

// json-schema.ts
if (skipAssertJSONValue) {
  return valita.ok(v as ReadonlyJSONValue);  // No validation!
}
```

**Impact**: Defense-in-depth validation is disabled. Practical impact is low because:
- Data arrives via JSON.parse() which already enforces JSON types
- Modern JSON.parse() doesn't pollute prototypes via `__proto__`

**Recommendation**: Consider keeping validation enabled in production for defense-in-depth, or document the security rationale.

---

### M3: 30-Day JWT Token Expiry

**File**: `apps/zbugs/api/index.ts:116`

**Issue**: Tokens are valid for 30 days, providing a long window for token theft/misuse.

```typescript
.setExpirationTime('30days')
```

**Impact**: If a token is compromised (via XSS, logging, session fixation), it remains valid for up to 30 days.

**Recommendation**:
- Reduce to 24 hours or less for sensitive operations
- Implement token refresh mechanism
- Add token revocation capability

---

### M4: Unauthenticated Unsubscribe Endpoint

**File**: `apps/zbugs/api/index.ts:253-298`

**Issue**: The `/api/unsubscribe` endpoint allows unsubscribing any user from any issue using only their email address.

```typescript
fastify.get<{
  Querystring: {id: string; email: string};
}>('/api/unsubscribe', async (request, reply) => {
  // No authentication! Just email lookup
  const existingUser = await sql`SELECT id FROM "user" WHERE "email" = ${request.query.email}`;
  // ... unsubscribes user
});
```

**Impact**: Attackers can unsubscribe users from issues by knowing their email address.

**Recommendation**:
- Add signed tokens to unsubscribe links
- Or require authentication
- Or implement rate limiting per email

---

## Low Severity Findings

### L1: Error Class Name Exposure

**File**: `packages/shared/src/error.ts:64-66`

**Issue**: Error responses expose internal class names when `error.name !== 'Error'`.

```typescript
if (error.name && error.name !== 'Error') {
  return {name: error.name};  // Exposes: PostgresError, ValidationError, etc.
}
```

**Impact**: Information disclosure that aids reconnaissance. Attackers learn:
- Database type (PostgresError)
- Validation framework
- Internal error handling structure

**Recommendation**: Map internal error types to generic error categories.

---

### L2: TOCTOU in Authorization Checks

**File**: `apps/zbugs/shared/auth.ts:39-60`, `apps/zbugs/shared/mutators.ts:102-115`

**Issue**: Time-of-check to time-of-use gap exists between authorization queries and mutations.

```typescript
// Auth check reads entity
const creatorID = must(await tx.run(query.where('id', id).one())).creatorID;

// Later, mutation happens on potentially different data
await tx.mutate.issue.update(change);
```

**Mitigating Factors**:
- Operations run within database transactions
- Serializable isolation likely prevents most races

**Recommendation**: Verify transaction isolation level is SERIALIZABLE for mutation transactions.

---

### L3: Mutation Name Has No Length Limit

**File**: `packages/zero-protocol/src/push.ts:84`

**Issue**: Mutation names are only validated as strings with no length limit.

```typescript
name: v.string(),  // No maxLength
```

**Impact**: Potential DoS via extremely long mutation names causing memory/logging issues.

**Recommendation**: Add maximum length validation (e.g., 256 characters).

---

## Findings Verified as Non-Issues

### SQL Injection: NOT VULNERABLE

**Files**:
- `packages/z2s/src/compiler.ts:394` - `__dangerous__rawValue` for operators
- `packages/zero-server/src/custom.ts:350-429` - CRUD SQL generation

**Analysis**:
1. SQL operators at `compiler.ts:394` are from a switch-case allowlist (validated by AST schema at `packages/zero-protocol/src/ast.ts:50-55`)
2. CRUD operations use `sql.ident()` for identifiers and parameterized values throughout
3. Table/column names come from schema definitions, not user input

---

### JWT Algorithm Confusion: NOT VULNERABLE

**File**: `apps/zbugs/api/index.ts:317`

**Analysis**: Uses `jose` library's `jwtVerify()` which:
- Validates algorithm against key type
- Rejects `alg: "none"` by default
- Properly validates signatures

---

## Remediation Priority

1. **Immediate** (H1, H2): Fix authorization bypass issues
2. **Short-term** (M3, M4): Reduce token expiry, secure unsubscribe endpoint
3. **Medium-term** (M1, M2): Improve prototype chain handling, review production validation
4. **Low priority** (L1, L2, L3): Address info leakage and DoS vectors

---

## Files Modified During Review

None - this was a read-only security review.
