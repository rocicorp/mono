# Security Review Phase 3.2 - Error Handling & Information Disclosure

## Executive Summary

This document contains the security review findings for Phase 3.2 (Error Handling & Information Disclosure):
- **6.1 Error Message Leakage** (zero-cache, zero-server)
- **6.2 Timing Attacks** (auth files, admin panel)

**Overall Assessment**: 2 medium-severity issues and 2 low-severity issues identified.

---

## Findings

### MEDIUM-01: Admin Password Vulnerable to Timing Attack

**Severity**: MEDIUM
**CVSS Score**: 5.3 (Medium)
**File**: `packages/zero-cache/src/config/zero-config.ts`
**Lines**: 909

**Vulnerability Description**:

The admin password validation uses direct string comparison instead of constant-time comparison:

```typescript
export function isAdminPasswordValid(
  lc: LogContext,
  config: Pick<NormalizedZeroConfig, 'adminPassword'>,
  password: string | undefined,
) {
  // ...

  if (password !== config.adminPassword) {  // TIMING VULNERABLE
    lc.warn?.('Invalid admin password');
    return false;
  }

  lc.debug?.('Admin password accepted');
  return true;
}
```

**Impact**:
- Standard string comparison (`!==`) returns as soon as a mismatch is found
- An attacker can measure response time differences to infer correct password characters
- Character-by-character brute force becomes feasible with sufficient measurements
- Network latency adds noise but high-precision attacks are still possible

**Attack Scenario**:
1. Attacker sends admin password attempts with varying first characters
2. Measures response times for each attempt
3. The correct first character takes slightly longer (comparison continues to second character)
4. Repeat for each character position

**Proof of Concept**:
```javascript
// Timing attack demonstration
async function timingAttack(url, correctChars, position) {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
  const timings = [];

  for (const char of chars) {
    const password = correctChars + char + 'x'.repeat(20);
    const times = [];

    for (let i = 0; i < 100; i++) {
      const start = performance.now();
      await fetch(url, {
        headers: { 'Authorization': `Basic ${btoa('admin:' + password)}` }
      });
      times.push(performance.now() - start);
    }

    timings.push({ char, avg: times.reduce((a, b) => a + b) / times.length });
  }

  // Character with highest average time is likely correct
  return timings.sort((a, b) => b.avg - a.avg)[0].char;
}
```

**Recommendation**:
Use `crypto.timingSafeEqual` for password comparison:

```typescript
import {timingSafeEqual} from 'node:crypto';

export function isAdminPasswordValid(
  lc: LogContext,
  config: Pick<NormalizedZeroConfig, 'adminPassword'>,
  password: string | undefined,
) {
  // ...

  if (!config.adminPassword) {
    lc.warn?.('No admin password set; denying access');
    return false;
  }

  // Use constant-time comparison
  const passwordBuffer = Buffer.from(password ?? '');
  const configBuffer = Buffer.from(config.adminPassword);

  // Lengths must match for timingSafeEqual
  if (passwordBuffer.length !== configBuffer.length) {
    // Still do the comparison to maintain constant time
    timingSafeEqual(passwordBuffer, passwordBuffer);
    lc.warn?.('Invalid admin password');
    return false;
  }

  if (!timingSafeEqual(passwordBuffer, configBuffer)) {
    lc.warn?.('Invalid admin password');
    return false;
  }

  lc.debug?.('Admin password accepted');
  return true;
}
```

---

### MEDIUM-02: Error Messages Leak Internal Information

**Severity**: MEDIUM
**CVSS Score**: 4.3 (Medium)
**Files**:
- `packages/shared/src/error.ts` (getErrorMessage, getErrorDetails)
- `packages/zero-server/src/process-mutations.ts`
- `packages/zero-cache/src/services/mutagen/pusher.ts`
- `packages/zero-cache/src/custom/fetch.ts`

**Vulnerability Description**:

Error messages from internal exceptions are passed through to clients without sanitization:

```typescript
// shared/src/error.ts
export function getErrorMessage(error: unknown): string {
  // ...
  if (error instanceof Error) {
    if (error.message) {
      return error.message;  // Returns raw error message
    }
  }
  // ...
}

// process-mutations.ts:199-208
const message = `Failed to parse push body: ${getErrorMessage(error)}`;
const details = getErrorDetails(error);
return {
  kind: ErrorKind.PushFailed,
  origin: ErrorOrigin.Server,
  reason: ErrorReason.Parse,
  message,
  mutationIDs,
  ...(details ? {details} : {}),  // Potentially sensitive details
} as const satisfies PushFailedBody;

// pusher.ts:524
message: `Failed to push: ${getErrorMessage(e)}`,
```

**Impact**:
- SQL error messages expose table names, column names, and query structure
- Database errors reveal schema information
- File system errors may expose internal paths
- Stack traces (via getErrorDetails) can reveal code structure
- PostgresError messages include detailed constraint information

**Examples of Leaked Information**:

1. **SQL Error Leakage**:
```
PostgresError: insert or update on table "fk_ref" violates foreign key constraint "fk_ref_ref_fkey"
```
Reveals: table name, constraint name, relationship structure

2. **Parse Error Leakage**:
```
Failed to parse push body: Unexpected token 'x' at position 42
```
Reveals: JSON parsing internals

3. **Internal Path Leakage**:
```
ENOENT: no such file or directory, open '/home/app/zero-cache/data/replica.db'
```
Reveals: internal file paths

**Recommendation**:

1. Create sanitized error messages for client-facing responses:

```typescript
const CLIENT_SAFE_MESSAGES: Record<ErrorReason, string> = {
  [ErrorReason.Parse]: 'Invalid request format',
  [ErrorReason.Database]: 'Database operation failed',
  [ErrorReason.Internal]: 'Internal server error',
  // ...
};

function getSafeErrorMessage(reason: ErrorReason, _error: unknown): string {
  return CLIENT_SAFE_MESSAGES[reason] ?? 'An error occurred';
}
```

2. Log detailed errors server-side, return generic messages to clients:

```typescript
lc.error?.('Detailed error:', error);  // Full details in logs
return {
  kind: ErrorKind.PushFailed,
  message: 'Database operation failed',  // Generic client message
  // Don't include details field for non-application errors
};
```

---

### LOW-01: Error Details Can Expose Error Names

**Severity**: LOW
**CVSS Score**: 3.1 (Low)
**File**: `packages/shared/src/error.ts`
**Lines**: 56-82

**Vulnerability Description**:

The `getErrorDetails()` function exposes error class names:

```typescript
export function getErrorDetails(error: unknown): ReadonlyJSONValue | undefined {
  if (error instanceof Error) {
    // ...
    if (error.name && error.name !== 'Error') {
      return {name: error.name};  // Exposes error class name
    }
  }
  // ...
}
```

**Impact**:
- Error class names like `PostgresError`, `SqliteError`, `AuthenticationError` reveal internal implementation
- Helps attackers understand which systems are in use
- Can be used to tailor attacks to specific database backends

**Recommendation**:
Don't expose error names to clients. Use generic error categories instead.

---

### LOW-02: Stack Traces in Observability Events

**Severity**: LOW
**CVSS Score**: 2.1 (Low)
**File**: `packages/zero-cache/src/observability/events.ts`
**Lines**: 117-132

**Vulnerability Description**:

The `makeErrorDetails()` function includes full stack traces:

```typescript
export function makeErrorDetails(e: unknown): JSONObject {
  const err = e instanceof Error ? e : new Error(String(e));
  const errorDetails: JSONObject = {
    name: err.name,
    message: err.message,
    stack: err.stack,  // Full stack trace
    cause: err.cause ? makeErrorDetails(err.cause) : undefined,
  };
  // ...
}
```

**Impact**:
- Stack traces reveal internal code structure, file paths, and function names
- If CloudEvents are exposed (misconfigured sink, log aggregation leak), this information becomes accessible
- Helps attackers understand application architecture

**Risk Assessment**:
- This is used for internal observability events, not client-facing errors
- Risk depends on how CloudEvents are configured and secured
- Default implementation logs to console, which should be protected

**Recommendation**:
1. Ensure CloudEvent sinks are properly secured
2. Consider redacting file paths from stack traces in production
3. Add documentation warning about stack trace exposure in events

---

## Positive Findings

### JWT Timing Attacks - NOT VULNERABLE

**Status**: SAFE
**File**: `packages/zero-cache/src/auth/jwt.ts`

The JWT validation uses the `jose` library which implements constant-time comparison internally:

```typescript
import { jwtVerify } from 'jose';

async function verifyTokenImpl(
  token: string,
  verifyKey: Uint8Array | KeyLike | JWK,
  verifyOptions: JWTClaimVerificationOptions,
): Promise<JWTPayload> {
  const {payload} = await jwtVerify(token, verifyKey, verifyOptions);
  return payload;
}
```

The `jose` library is a well-maintained, security-focused JWT library that:
- Uses constant-time signature comparison
- Handles algorithm confusion attacks
- Properly validates claims

### Error Handling Structure - GOOD

The codebase has a well-structured error handling system:

1. **ProtocolError class**: Standardized error format with kind, message, and origin
2. **Error wrapping**: Internal errors are wrapped before sending to clients
3. **Error logging**: Errors are logged with appropriate severity levels
4. **Categorization**: Errors are categorized by origin (Server, ZeroCache)

---

## Timing Attack Analysis

### Permission Check Timing

**Status**: INFORMATIONAL
**Files**: `read-authorizer.ts`, `write-authorizer.ts`

Authorization checks involve database queries which naturally have variable timing. However:

- The primary attack vector (response timing) already leaks less information than the query result
- Query timing depends on data volume, not authorization decisions
- No early-exit patterns that would leak authorization state via timing

**Conclusion**: Permission check timing is not a significant security concern in this context.

---

## Remediation Priority

| Finding | Severity | Effort | Priority |
|---------|----------|--------|----------|
| MEDIUM-01: Admin password timing attack | Medium | Low | P1 |
| MEDIUM-02: Error message information leak | Medium | Medium | P1 |
| LOW-01: Error names exposure | Low | Low | P2 |
| LOW-02: Stack traces in events | Low | Low | P3 |

---

## Recommended Test Cases

### For MEDIUM-01 (Timing Attack):
```typescript
test('password comparison is constant-time', async () => {
  // This is difficult to test reliably, but we can verify
  // timingSafeEqual is used by checking the implementation
  const code = isAdminPasswordValid.toString();
  expect(code).toContain('timingSafeEqual');
});
```

### For MEDIUM-02 (Error Leakage):
```typescript
test('error messages do not contain SQL details', async () => {
  // Trigger a database error
  const response = await triggerDatabaseError();

  // Verify no SQL-specific terms in client-facing message
  expect(response.message).not.toMatch(/PostgresError|column|constraint|table/i);
  expect(response.details).toBeUndefined();
});

test('error messages do not contain file paths', async () => {
  const response = await triggerFileError();
  expect(response.message).not.toMatch(/\/home\/|\/app\/|\.ts|\.js/);
});
```

---

## Files Reviewed

| File | Purpose | Findings |
|------|---------|----------|
| `packages/zero-cache/src/config/zero-config.ts` | Admin password validation | Timing attack |
| `packages/shared/src/error.ts` | Error message extraction | Information leakage |
| `packages/zero-server/src/process-mutations.ts` | Mutation error handling | Uses getErrorMessage |
| `packages/zero-cache/src/services/mutagen/pusher.ts` | Push error handling | Uses getErrorMessage |
| `packages/zero-cache/src/custom/fetch.ts` | Custom fetch errors | Uses getErrorMessage |
| `packages/zero-cache/src/types/error-with-level.ts` | Error wrapping | Uses getErrorMessage |
| `packages/zero-cache/src/observability/events.ts` | Event error details | Stack trace exposure |
| `packages/zero-cache/src/auth/jwt.ts` | JWT validation | Safe (jose library) |
| `packages/zero-cache/src/auth/read-authorizer.ts` | Read authorization | No timing issues |
| `packages/zero-cache/src/auth/write-authorizer.ts` | Write authorization | No timing issues |

---

## Appendix: Error Information Flow

```
Exception Thrown
       │
       ▼
┌──────────────────┐
│ getErrorMessage()│  ← Extracts raw error message
│ getErrorDetails()│  ← Extracts name, details
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  PushFailedBody  │  ← message and details fields
│  ErrorBody       │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│   WebSocket      │  ← Sent to client
│   sendError()    │
└──────────────────┘
```

This flow shows how internal error details can leak to clients through the error handling chain.
