# WebSocket Security Review - `packages/zero-cache`

## Executive Summary

The WebSocket implementation in zero-cache is generally well-structured with proper message validation and authentication patterns. However, I identified several security concerns ranging from medium to informational severity.

---

## Findings

### 1. **[MEDIUM] Potential Token Logging in Debug Logs**

**Location:** `packages/zero-cache/src/workers/syncer.ts:169-171`

```typescript
this.#lc.debug?.(
  `Received auth token ${auth} for clientID ${clientID}, decoded: ${JSON.stringify(decodedToken)}`,
);
```

**Issue:** The raw JWT token is logged in debug output. If debug logging is enabled in production, tokens could be exposed in log files.

**Impact:** Token theft if logs are compromised. JWTs may be valid for a period and could be replayed.

**Recommendation:** Never log raw tokens. Log only the token's claims or a truncated/hashed identifier.

---

### 2. **[MEDIUM] Authentication Bypass When Custom Endpoints Configured**

**Location:** `packages/zero-cache/src/workers/syncer.ts:146-161`

```typescript
const hasCustomEndpoints = hasPushOrMutate && hasQueries;
if (!hasExactlyOneTokenOption && !hasCustomEndpoints) {
  throw new Error(...)
}
```

**Issue:** When both `ZERO_MUTATE_URL` and `ZERO_QUERY_URL` are configured, token verification can be skipped even if an auth token is provided. The code at `syncer.ts:186-190` explicitly logs a warning but continues without verification:

```typescript
} else {
  this.#lc.warn?.(
    `One of jwk, secret, or jwksUrl is not configured - the \`authorization\` header must be manually verified by the user`,
  );
}
```

**Impact:** Connections may be established with unverified tokens if server operators misconfigure authentication. The decoded token passed to `SyncerWsMessageHandler` will be an empty object `{}` rather than `undefined`, potentially confusing downstream authorization logic.

**Recommendation:** Either require token verification when tokens are present, or make it clear in the code that `decodedToken` being `{}` vs `undefined` has different meanings.

---

### 3. **[MEDIUM] Missing Origin Validation on WebSocket Connections**

**Location:** Throughout WebSocket connection handling

**Issue:** There is no explicit Origin header validation on WebSocket upgrade requests. While the browser enforces same-origin policies, a malicious site could potentially initiate WebSocket connections to the Zero server if credentials are cookie-based.

**Impact:** Cross-Site WebSocket Hijacking (CSWSH) attacks if the server relies on cookies for authentication without validating the Origin.

**Recommendation:** Add explicit Origin header validation in the WebSocket handoff handler, especially when `httpCookie` is used for authentication.

---

### 4. **[MEDIUM] Rate Limiter Per ClientGroupID, Not Per User**

**Location:** `packages/zero-cache/src/services/mutagen/mutagen.ts:100-105`, `packages/zero-cache/src/services/limiter/sliding-window-limiter.ts`

```typescript
// Created per-MutagenService (one per clientGroupID)
if (config.perUserMutationLimit.max !== undefined) {
  this.#limiter = new SlidingWindowLimiter(
    config.perUserMutationLimit.windowMs,
    config.perUserMutationLimit.max,
  );
}
```

**Issue:** Despite being named `perUserMutationLimit`, the rate limiter is actually per `clientGroupID`, not per authenticated user. A malicious user could create multiple client groups to bypass rate limiting.

**Impact:** Rate limiting can be circumvented by creating multiple client groups, enabling mutation flooding.

**Recommendation:** Consider adding rate limiting keyed by authenticated user ID (from JWT `sub` claim) in addition to or instead of clientGroupID.

---

### 5. **[MEDIUM] User-Provided Push URL Validation**

**Location:** `packages/zero-cache/src/services/mutagen/pusher.ts:278-312`, `packages/zero-cache/src/custom/fetch.ts:78-96`

**Issue:** Users can provide `userPushURL` via `initConnectionMessage`, which is used for custom mutations. While there is URL pattern matching:

```typescript
if (!urlMatch(url, allowedUrlPatterns)) {
  throw new ProtocolErrorWithLevel(...)
}
```

The patterns are derived from `ZERO_MUTATE_URL` configuration. If the pattern is overly permissive (e.g., uses wildcards), an attacker could potentially direct requests to unintended endpoints.

**Impact:** Server-Side Request Forgery (SSRF) if URL patterns are misconfigured.

**Recommendation:** Document secure URL pattern configuration practices. Consider adding an allowlist mode for user-specified URLs.

---

### 6. **[LOW] No Connection Limits Per IP/User**

**Location:** `packages/zero-cache/src/workers/syncer.ts`

**Issue:** While individual connections are tracked by `clientID` and duplicate connections from the same client are handled (`syncer.ts:134-140`), there's no limit on the total number of connections from a single IP address or authenticated user.

**Impact:** Resource exhaustion through connection flooding from a single source.

**Recommendation:** Implement connection limits per IP and/or per authenticated user at the syncer or dispatcher level.

---

### 7. **[LOW] Error Message Information Disclosure**

**Location:** `packages/zero-cache/src/workers/connection.ts:189`, `packages/zero-cache/src/types/ws.ts:18-23`

```typescript
this.#lc.warn?.(`failed to parse message "${data}": ${String(e)}`);
```

```typescript
// close messages must be less than or equal to 123 bytes
ws.close(code, elide(errMsg, 123));
```

**Issue:** Error messages containing user input are logged and may be sent back to clients. The `elide` function truncates but doesn't sanitize the message content.

**Impact:** Information leakage about server internals. Could expose stack traces or internal paths in certain error conditions.

**Recommendation:** Sanitize error messages before logging/sending. Remove stack traces and internal paths from client-facing errors.

---

### 8. **[LOW] JSON Parsing Without Size Limits**

**Location:** `packages/zero-cache/src/workers/connection.ts:186`

```typescript
const value = JSON.parse(data);
msg = valita.parse(value, upstreamSchema);
```

**Issue:** Incoming WebSocket messages are parsed with `JSON.parse()` without explicit size limits. While the protocol schema validation (Valita) rejects malformed data, parsing happens before validation.

**Impact:** Large JSON payloads could cause memory pressure during parsing.

**Recommendation:** Consider implementing message size limits at the WebSocket level before JSON parsing.

---

### 9. **[LOW] Remote JWK Set Caching Without TTL**

**Location:** `packages/zero-cache/src/auth/jwt.ts:31-38`

```typescript
let remoteKeyset: ReturnType<typeof createRemoteJWKSet> | undefined;
function getRemoteKeyset(jwksUrl: string) {
  if (remoteKeyset === undefined) {
    remoteKeyset = createRemoteJWKSet(new URL(jwksUrl));
  }
  return remoteKeyset;
}
```

**Issue:** The remote JWKS is cached indefinitely at the module level. If keys are rotated, the server would need to restart to fetch new keys. Also, only one JWKS URL is ever cached.

**Impact:** Stale keys could prevent legitimate authentication after key rotation, or old compromised keys might remain trusted.

**Recommendation:** Use `jose`'s built-in cache options with appropriate TTL, or implement explicit cache invalidation.

---

### 10. **[INFO] Deprecated Authentication Module**

**Location:** `packages/zero-cache/src/auth/jwt.ts:13`, `packages/zero-cache/src/auth/jwt.ts:40`, `packages/zero-cache/src/auth/jwt.ts:49`

```typescript
/** @deprecated */
export async function createJwkPair() { ... }
/** @deprecated */
export const tokenConfigOptions = ...
/** @deprecated */
export async function verifyToken(...) { ... }
```

**Issue:** The JWT verification functions are marked as deprecated, suggesting a migration to a different auth mechanism. Deprecated security code may receive less attention during security reviews.

**Impact:** Unclear migration path could lead to inconsistent security controls.

**Recommendation:** Complete the migration to the new auth mechanism or remove deprecation markers if the code will continue to be maintained.

---

### 11. **[INFO] Secure Protocol Header Encoding**

**Location:** `packages/zero-protocol/src/connect.ts:58-86`

The `Sec-WebSocket-Protocol` header is used to transmit the `initConnectionMessage` and `authToken` via base64 encoding:

```typescript
export function decodeSecProtocols(secProtocol: string): {
  initConnectionMessage: InitConnectionMessage | undefined;
  authToken: string | undefined;
} {
  const binString = atob(decodeURIComponent(secProtocol));
  const bytes = Uint8Array.from(binString, c => c.charCodeAt(0));
  return JSON.parse(new TextDecoder().decode(bytes));
}
```

**Observation:** The decoded JSON is not validated against a schema before being returned. While the `initConnectionMessage` is later validated against `initConnectionMessageSchema`, the `authToken` is used directly.

---

## Positive Security Features

1. **Schema Validation**: All incoming messages are validated against Valita schemas (`upstreamSchema`) before processing
2. **Heartbeat/Liveness**: Proper ping/pong mechanism prevents zombie connections (`packages/zero-cache/src/types/ws.ts:26-93`)
3. **ClientGroupID Validation**: Mutations validate that `clientGroupID` matches the connection (`packages/zero-cache/src/workers/syncer-ws-message-handler.ts:95-108`)
4. **SQL Injection Protection**: Uses parameterized queries with `postgres` library's tagged templates (`packages/zero-cache/src/services/mutagen/mutagen.ts:382-428`)
5. **Write Authorization**: Implements pre/post mutation authorization checks (`packages/zero-cache/src/auth/write-authorizer.ts`)
6. **Graceful Shutdown**: Drain coordination prevents cascading reconnections (`packages/zero-cache/src/workers/syncer.ts:262-281`)
7. **URL Pattern Validation**: Custom mutation URLs are validated against configured patterns (`packages/zero-cache/src/custom/fetch.ts:78-96`)

---

## Summary

| Severity | Count |
|----------|-------|
| High | 0 |
| Medium | 5 |
| Low | 4 |
| Informational | 2 |

The most critical areas for improvement are:
1. Removing raw token logging
2. Clarifying authentication behavior when custom endpoints are used
3. Adding Origin validation for WebSocket connections
4. Implementing true per-user rate limiting