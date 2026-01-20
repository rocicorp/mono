# Security Review Phase 2.4 - WebSocket Protocol Parsing

## Executive Summary

This document contains the security review findings for Phase 2.4 (WebSocket Protocol Parsing):
- **2.4 WebSocket Protocol Parsing** (zero-cache)

**Overall Assessment**: 2 low-severity issues and 1 informational finding identified. The protocol parsing is generally well-implemented with good error handling.

---

## Findings

### LOW-01: No Schema Validation on Decoded sec-websocket-protocol

**Severity**: LOW
**CVSS Score**: 3.7 (Low)
**File**: `packages/zero-protocol/src/connect.ts:79-86`

**Vulnerability Description**:

The `decodeSecProtocols` function decodes and parses JSON from the sec-websocket-protocol header without validating against the expected schema:

```typescript
export function decodeSecProtocols(secProtocol: string): {
  initConnectionMessage: InitConnectionMessage | undefined;
  authToken: string | undefined;
} {
  const binString = atob(decodeURIComponent(secProtocol));
  const bytes = Uint8Array.from(binString, c => c.charCodeAt(0));
  return JSON.parse(new TextDecoder().decode(bytes));  // No validation!
}
```

**Attack Vector**:
An attacker could send a malformed protocol header containing unexpected properties:
```javascript
// Attacker's payload
{
  "initConnectionMessage": ["initConnection", {...}],
  "authToken": "valid-token",
  "__proto__": {"polluted": true},  // Extra property
  "unexpectedField": "malicious"
}
```

**Impact**:
- The returned object could contain unexpected properties
- Downstream code assumes the shape matches `InitConnectionMessage` and `authToken`
- While TypeScript provides compile-time safety, runtime validation is missing
- No prototype pollution possible (JSON.parse doesn't assign to prototype)

**Existing Mitigation**:
- The `initConnectionMsg` is later validated with `valita.parse()` in the message handler
- TypeScript types provide compile-time safety

**Recommendation**:

Add runtime validation using valita:

```typescript
const secProtocolsSchema = v.object({
  initConnectionMessage: initConnectionMessageSchema.optional(),
  authToken: v.string().optional(),
});

export function decodeSecProtocols(secProtocol: string): {
  initConnectionMessage: InitConnectionMessage | undefined;
  authToken: string | undefined;
} {
  const binString = atob(decodeURIComponent(secProtocol));
  const bytes = Uint8Array.from(binString, c => c.charCodeAt(0));
  const json = JSON.parse(new TextDecoder().decode(bytes));
  return v.parse(json, secProtocolsSchema);  // Validates schema
}
```

---

### LOW-02: Integer Parsing Allows Floating Point and Negative Values

**Severity**: LOW
**CVSS Score**: 2.0 (Low)
**Files**:
- `packages/zero-cache/src/server/worker-dispatcher.ts:36-39`
- `packages/zero-cache/src/types/url-params.ts:21-35`

**Vulnerability Description**:

Protocol version parsing uses `Number()` which accepts floating point values:

```typescript
// worker-dispatcher.ts:36
const version = Number(path.version);
if (Number.isNaN(version)) {
  throw new Error(`Invalid version: ${u}`);
}
```

The `URLParams.getInteger()` uses `parseInt()` which truncates decimals:

```typescript
// url-params.ts:28
const int = parseInt(value);
if (isNaN(int)) {
  throw new Error(...);
}
return int;
```

**Attack Scenarios**:

| Input URL | `Number()` Result | `parseInt()` Result | Behavior |
|-----------|-------------------|---------------------|----------|
| `/sync/v1.9/connect` | `1.9` | `1` | Comparison fails (1.9 < 30) |
| `/sync/v-5/connect` | `-5` | `-5` | Comparison fails (-5 < 30) |
| `/sync/v30.0/connect` | `30` | `30` | Would pass (edge case) |
| `/sync/v0x1e/connect` | `NaN` | `30` (hex) | parseInt parses hex! |

**Impact**:
- Low severity due to range validation at `connection.ts:127-148`
- Version must be within `[MIN_SERVER_SUPPORTED_SYNC_PROTOCOL, PROTOCOL_VERSION]` range
- `parseInt` accepting hex could allow `"0x1e"` to parse as `30` (bypassing intent)

**Recommendation**:

Use stricter integer parsing:

```typescript
function parseStrictInteger(value: string): number {
  if (!/^-?\d+$/.test(value)) {
    throw new Error(`Invalid integer: ${value}`);
  }
  const num = Number(value);
  if (!Number.isInteger(num) || !Number.isSafeInteger(num)) {
    throw new Error(`Invalid integer: ${value}`);
  }
  return num;
}
```

---

### INFO-01: Header Size Limit Provides DoS Protection

**Severity**: INFORMATIONAL
**Files**: Node.js HTTP Server defaults

**Description**:

The sec-websocket-protocol header could theoretically contain large base64-encoded payloads. However, Node.js has default header size limits that provide protection:

- **Default Node.js max header size**: 16KB (16,384 bytes)
- **Fastify**: Inherits Node.js defaults

A 16KB header limit means:
- Max base64 payload: ~12KB (after base64 decoding)
- Max JSON payload: ~12KB

This is sufficient for `initConnectionMessage` with typical `desiredQueriesPatch` arrays.

**Security Implications**:
- Attackers cannot send multi-megabyte protocol headers
- Connection upgrade fails before reaching application code
- Memory exhaustion via oversized headers is prevented

**Recommendation**:
- Document the implicit dependency on Node.js header limits
- Consider adding explicit `maxHeaderSize` configuration for production hardening
- The connect.test.ts test for 1MB data tests encoding only, not server-side decoding

---

## Security Strengths Observed

### Protocol Version Validation (GOOD)

The protocol version is validated at multiple points:

1. **URL Parsing** (worker-dispatcher.ts:36-39):
   - `Number()` conversion with NaN check
   - Rejects non-numeric versions

2. **Range Validation** (connection.ts:127-148):
   ```typescript
   if (
     this.#protocolVersion > PROTOCOL_VERSION ||
     this.#protocolVersion < MIN_SERVER_SUPPORTED_SYNC_PROTOCOL
   ) {
     this.#closeWithError({...});
   }
   ```

3. **Clear Error Messages**:
   - Tells client whether server or client needs updating
   - Uses `ErrorKind.VersionNotSupported`

### Error Handling During Upgrade (GOOD)

The handoff error handling in `websocket-handoff.ts:83-93` is well-designed:

```typescript
function onError(error: unknown) {
  // Returning an error on the HTTP handshake looks like a hanging connection
  // (at least from Chrome) and doesn't report any meaningful error in the browser.
  // Instead, finish the upgrade to a websocket and then close it with an error.
  wss.handleUpgrade(
    message as IncomingMessage,
    socket,
    Buffer.from(head),
    ws => closeWithError(lc, ws, error, PROTOCOL_ERROR),
  );
}
```

This approach:
1. Completes the WebSocket upgrade
2. Closes with a proper error code (1002 PROTOCOL_ERROR)
3. Provides meaningful error messages to clients

### Socket Existence Check (GOOD)

The `installWebSocketReceiver` function (websocket-handoff.ts:142-146) properly checks socket existence:

```typescript
if (!socket) {
  lc.warn?.('websocket closed during handoff');
  return;
}
```

This handles the race condition where the connection closes during inter-process handoff.

### Close Message Length Handling (GOOD)

The `closeWithError` function (ws.ts:21-23) properly handles WebSocket close message limits:

```typescript
// close messages must be less than or equal to 123 bytes:
// https://developer.mozilla.org/en-US/docs/Web/API/WebSocket/close#reason
ws.close(code, elide(errMsg, 123));
```

---

## Remediation Priority

| Finding | Severity | Effort | Priority |
|---------|----------|--------|----------|
| LOW-01: No schema validation on decoded protocol | Low | Low | P3 |
| LOW-02: Integer parsing allows float/negative | Low | Low | P3 |
| INFO-01: Header size limit (informational) | Info | N/A | Document |

---

## Recommended Test Cases

### For LOW-01 (Schema Validation):
```typescript
test('rejects malformed sec-websocket-protocol', () => {
  const malformed = encodeURIComponent(btoa(JSON.stringify({
    initConnectionMessage: "not-an-array",  // Wrong type
    authToken: 123,  // Wrong type
  })));

  expect(() => decodeSecProtocols(malformed)).toThrow();
});

test('rejects extra properties in sec-websocket-protocol', () => {
  const withExtra = encodeURIComponent(btoa(JSON.stringify({
    initConnectionMessage: undefined,
    authToken: undefined,
    __proto__: {},  // Should be stripped or rejected
  })));

  const result = decodeSecProtocols(withExtra);
  expect(Object.keys(result)).toEqual(['initConnectionMessage', 'authToken']);
});
```

### For LOW-02 (Integer Parsing):
```typescript
test.each([
  ['1.5', false],
  ['-1', false],
  ['0x1e', false],  // hex
  ['1e2', false],   // scientific notation
  ['30', true],
  ['45', true],
])('parseProtocolVersion(%s) valid=%s', (input, isValid) => {
  if (isValid) {
    expect(parseProtocolVersion(input)).toBeTypeOf('number');
  } else {
    expect(() => parseProtocolVersion(input)).toThrow();
  }
});
```

---

## Files Reviewed

| File | Purpose | Findings |
|------|---------|----------|
| `packages/zero-protocol/src/connect.ts` | Protocol encoding/decoding | No schema validation |
| `packages/zero-cache/src/workers/connect-params.ts` | Connection parameter extraction | Uses decoded protocol |
| `packages/zero-cache/src/server/worker-dispatcher.ts` | URL routing | Float version parsing |
| `packages/zero-cache/src/types/url-params.ts` | URL parameter parsing | parseInt issues |
| `packages/zero-cache/src/types/websocket-handoff.ts` | WS upgrade handling | Good error handling |
| `packages/zero-cache/src/workers/connection.ts` | Connection management | Good version validation |
| `packages/zero-cache/src/types/ws.ts` | WebSocket utilities | Good close handling |
| `packages/zero-protocol/src/protocol-version.ts` | Version constants | Well documented |

---

## Appendix: Base64 Encoding Considerations

The `encodeSecProtocols` function (connect.ts:58-77) properly handles:

1. **Unicode strings**: UTF-8 encoding before base64
2. **URI encoding**: `encodeURIComponent()` for WebSocket protocol header
3. **Large data**: Avoids `String.fromCharCode.apply()` stack overflow

However, decoding (`decodeSecProtocols`) performs these operations synchronously:
- `decodeURIComponent()` - O(n) string operation
- `atob()` - Base64 decoding
- `Uint8Array.from()` - Array creation with callback
- `TextDecoder.decode()` - UTF-8 decoding
- `JSON.parse()` - JSON parsing

For a 16KB header (Node.js limit), these operations are fast. But if header limits were increased, this could become a CPU-bound DoS vector.
