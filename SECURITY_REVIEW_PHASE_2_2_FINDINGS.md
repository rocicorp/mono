# Security Review Phase 2.2 - JSON Parsing DoS

## Executive Summary

This document contains the security review findings for Phase 2.2 (JSON Parsing DoS):
- **2.2 JSON Parsing DoS** (zero-cache)

**Overall Assessment**: 3 medium-severity issues and 1 high-severity design concern identified. The primary risks are denial-of-service via resource exhaustion.

---

## Findings

### MEDIUM-01: No WebSocket Message Size Limit

**Severity**: MEDIUM
**CVSS Score**: 5.3 (Medium)
**File**: `packages/zero-cache/src/workers/syncer.ts`
**Lines**: 40-63, 114

**Vulnerability Description**:

The WebSocket server is created without specifying a `maxPayload` limit:

```typescript
function getWebSocketServerOptions(config: ZeroConfig): ServerOptions {
  const options: ServerOptions = {
    noServer: true,
  };
  // No maxPayload specified!
  if (config.websocketCompression) {
    options.perMessageDeflate = true;
    // ...
  }
  return options;
}

// Line 114:
this.#wss = new WebSocketServer(getWebSocketServerOptions(config));
```

**Impact**:
- The `ws` library default `maxPayload` is 100MB (104,857,600 bytes)
- An attacker can send 100MB JSON payloads repeatedly to exhaust server memory
- With multiple concurrent connections, memory exhaustion is amplified
- Even with backpressure, parsing 100MB JSON strings consumes significant CPU

**Attack Scenario**:
1. Attacker establishes WebSocket connection
2. Sends valid-looking but maximally sized JSON payloads (100MB each)
3. Server's `JSON.parse()` allocates memory for the string and parsed object
4. Repeat across multiple connections to exhaust memory/CPU

**Recommendation**:
1. Set `maxPayload` to a reasonable limit (e.g., 1MB or 10MB based on expected message sizes)
2. Add configuration option `ZERO_MAX_MESSAGE_SIZE` to allow operators to tune
3. Example fix:
```typescript
const options: ServerOptions = {
  noServer: true,
  maxPayload: config.maxMessageSize ?? 10 * 1024 * 1024, // 10MB default
};
```

---

### MEDIUM-02: Blocking JSON.parse Without Timeout

**Severity**: MEDIUM
**CVSS Score**: 5.3 (Medium)
**File**: `packages/zero-cache/src/workers/connection.ts`
**Lines**: 177-199

**Vulnerability Description**:

The `#handleMessage` method calls `JSON.parse()` synchronously on the main event loop without any timeout protection:

```typescript
#handleMessage = async (event: {data: Data}) => {
  const data = event.data.toString();  // String conversion
  // ...
  let msg;
  try {
    const value = JSON.parse(data);  // BLOCKING - no timeout
    msg = valita.parse(value, upstreamSchema);
  } catch (e) {
    // ...
  }
  // ...
};
```

**Impact**:
- `JSON.parse()` is synchronous and blocks the Node.js event loop
- Large or complex JSON payloads can block the event loop for seconds
- During this time, all other connections on the same worker are stalled
- No keepalive messages can be sent, potentially causing legitimate clients to timeout

**Complexity Analysis**:
- Parsing a 10MB JSON string takes approximately 50-200ms on modern hardware
- Parsing a 100MB JSON string can take 500ms-2s
- During parsing, no other async operations can execute

**Recommendation**:
1. Consider using streaming JSON parsers for large payloads (e.g., `stream-json`)
2. Add a message size check before parsing and reject oversized messages early
3. For defense in depth, implement worker process monitoring to detect stuck workers

---

### MEDIUM-03: Production Mode Skips JSON Value Validation

**Severity**: MEDIUM
**CVSS Score**: 4.3 (Medium)
**Files**:
- `packages/shared/src/config.ts`
- `packages/shared/src/json.ts`
- `packages/shared/src/json-schema.ts`

**Vulnerability Description**:

In production mode (`NODE_ENV=production`), all JSON value validation is skipped:

```typescript
// config.ts
export const isProd = process.env.NODE_ENV === 'production';
export {isProd as skipAssertJSONValue};

// json.ts:130-133
export function assertJSONValue(v: unknown): asserts v is JSONValue {
  if (skipAssertJSONValue) {
    return;  // NO VALIDATION IN PRODUCTION!
  }
  // ... validation logic
}

// json-schema.ts:11-14
export const jsonSchema: valita.Type<ReadonlyJSONValue> = v
  .unknown()
  .chain(v => {
    if (skipAssertJSONValue) {
      return valita.ok(v as ReadonlyJSONValue);  // NO VALIDATION IN PRODUCTION!
    }
    // ... validation logic
  });
```

**Impact**:
- In development, JSON values are validated to ensure they contain only valid JSON types
- In production, this validation is completely bypassed for performance
- Malformed data (functions, symbols, circular references) could theoretically propagate
- While `JSON.parse()` output is always valid JSON, downstream code assumes validation occurred

**Risk Assessment**:
- This is a **defense in depth** issue, not a direct vulnerability
- `JSON.parse()` will always produce valid JSON types, so the skipped validation doesn't directly enable attacks
- However, it creates technical debt and potential confusion about what guarantees the type system provides

**Recommendation**:
1. Document this design decision clearly in code comments
2. Consider renaming `skipAssertJSONValue` to `skipAssertJSONValueInProduction` for clarity
3. Add integration tests that specifically verify production behavior

---

### MEDIUM-04: No JSON Nesting Depth Limit

**Severity**: MEDIUM
**CVSS Score**: 5.3 (Medium)
**File**: `packages/zero-cache/src/workers/connection.ts`
**Lines**: 186

**Vulnerability Description**:

The codebase accepts deeply nested JSON without any explicit depth limit:

```typescript
const value = JSON.parse(data);  // No depth limit check
```

**Technical Details**:
- V8's `JSON.parse()` can handle JSON nested to approximately 5,000-10,000 levels
- Beyond this, V8 throws a stack overflow error
- While this provides a natural limit, the limit is implementation-dependent

**Impact**:
- Deeply nested JSON (e.g., 5000 levels) can be sent by attackers
- While `JSON.parse()` handles this, subsequent processing (recursive algorithms, stringify operations) may cause stack overflows
- Different Node.js versions or V8 updates could change the threshold
- Recursive functions processing nested JSON (e.g., `deepEqual`, `assertJSONValue`) can stack overflow

**Proof of Concept**:
```javascript
// Generate deeply nested JSON
const depth = 5000;
let json = '{"a":';
for (let i = 0; i < depth; i++) json += '{"a":';
json += 'null';
for (let i = 0; i < depth; i++) json += '}';
json += '}';
// Send this as a WebSocket message
```

**Recommendation**:
1. Implement explicit depth limit checking (e.g., max 100 levels)
2. Add `JSON.parse` reviver function to track and limit depth
3. Example implementation:
```typescript
function parseJSONWithDepthLimit(data: string, maxDepth: number = 100): unknown {
  let depth = 0;
  return JSON.parse(data, (key, value) => {
    if (typeof value === 'object' && value !== null) {
      depth++;
      if (depth > maxDepth) {
        throw new Error(`JSON nesting depth exceeds limit of ${maxDepth}`);
      }
    }
    return value;
  });
}
```

---

## Design Concern

### HIGH-DESIGN: Combined DoS Attack Surface

**Severity**: HIGH (Combined Impact)

When the above issues are combined, they create a significant DoS attack surface:

1. **Attack Vector 1 - Memory Exhaustion**:
   - Send 100MB messages across multiple connections
   - Each message consumes ~200-300MB RAM (string + parsed object + temporary allocations)
   - 10 concurrent connections = 2-3GB memory pressure

2. **Attack Vector 2 - Event Loop Starvation**:
   - Send moderately large (~10MB) deeply nested JSON
   - Parsing blocks event loop for hundreds of milliseconds
   - Legitimate clients experience timeouts and disconnections

3. **Attack Vector 3 - Stack Overflow**:
   - Send deeply nested JSON (~5000 levels)
   - Trigger stack overflow in recursive processing functions
   - Worker process crashes, requiring restart

**Combined Recommendation**:
Implement defense in depth with multiple layers:
1. **Network Layer**: Set `maxPayload: 1_000_000` (1MB) on WebSocket server
2. **Application Layer**: Validate JSON depth before processing
3. **Process Layer**: Add worker process health monitoring
4. **Operational Layer**: Add rate limiting per client/IP

---

## Security Strengths Observed

### Input Validation with valita (GOOD)

After `JSON.parse()`, messages are validated against strict schemas:

```typescript
msg = valita.parse(value, upstreamSchema);
```

This provides:
1. Type validation for message structure
2. Rejection of unexpected message types
3. Protection against prototype pollution (valita doesn't assign to prototype)

### Error Handling (GOOD)

JSON parsing errors are properly caught and handled:

```typescript
} catch (e) {
  this.#lc.warn?.(`failed to parse message "${data}": ${String(e)}`);
  this.#closeWithError(
    {
      kind: ErrorKind.InvalidMessage,
      message: String(e),
      origin: ErrorOrigin.ZeroCache,
    },
    e,
  );
  return;
}
```

This ensures:
1. Parse failures don't crash the server
2. Clients receive clear error messages
3. Errors are logged for monitoring

---

## Remediation Priority

| Finding | Severity | Effort | Priority |
|---------|----------|--------|----------|
| MEDIUM-01: No maxPayload limit | Medium | Low | P1 |
| MEDIUM-02: Blocking JSON.parse | Medium | Medium | P2 |
| MEDIUM-03: Production validation bypass | Medium | Low | P3 |
| MEDIUM-04: No depth limit | Medium | Low | P1 |

---

## Recommended Test Cases

### For MEDIUM-01 (Message Size):
```typescript
test('rejects messages exceeding maxPayload', async () => {
  const largePayload = 'x'.repeat(10 * 1024 * 1024 + 1); // 10MB + 1 byte
  // Verify WebSocket closes with appropriate error code
});
```

### For MEDIUM-04 (Depth Limit):
```typescript
test('rejects deeply nested JSON', async () => {
  const depth = 200;
  let json = '{"a":';
  for (let i = 0; i < depth; i++) json += '{"a":';
  json += 'null';
  for (let i = 0; i < depth + 1; i++) json += '}';
  // Verify error response about nesting limit
});
```

---

## Files Reviewed

| File | Purpose | Findings |
|------|---------|----------|
| `packages/zero-cache/src/workers/syncer.ts` | WebSocket server setup | No maxPayload |
| `packages/zero-cache/src/workers/connection.ts` | Message handling | Blocking JSON.parse |
| `packages/zero-cache/src/workers/syncer-ws-message-handler.ts` | Message dispatch | Receives parsed messages |
| `packages/shared/src/config.ts` | Config flags | skipAssertJSONValue |
| `packages/shared/src/json.ts` | JSON utilities | Validation bypass |
| `packages/shared/src/json-schema.ts` | valita schemas | Validation bypass |
| `packages/zero-protocol/src/up.ts` | Upstream message schema | Uses jsonSchema |
| `packages/zero-protocol/src/push.ts` | Push message schema | Uses jsonSchema |
| `packages/zero-protocol/src/data.ts` | Data schemas | Uses jsonSchema |

---

## Appendix: V8 JSON.parse Limits

V8's JSON.parse has implementation-defined limits:

- **String length**: Limited by V8's maximum string size (~512MB on 64-bit)
- **Nesting depth**: ~5,000-10,000 levels (limited by call stack)
- **Object keys**: No explicit limit, but memory-bound
- **Array elements**: No explicit limit, but memory-bound

These limits may change between V8 versions and should not be relied upon for security.
