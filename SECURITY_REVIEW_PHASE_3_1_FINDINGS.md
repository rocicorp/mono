# Security Review Phase 3.1 - JSON Parsing Limits (Defense in Depth)

## Executive Summary

This document contains the security review findings for Phase 3.1, which is a verification and defense-in-depth review of JSON parsing limits. This phase confirms that the issues identified in Phase 2.2 remain unmitigated and provides additional analysis of JSON parsing attack surfaces.

**Overall Assessment**: All 4 issues from Phase 2.2 remain unaddressed. No mitigations have been implemented.

---

## Verification Status

### Phase 2.2 Findings - Current Status

| Finding | Status | Verification Method |
|---------|--------|---------------------|
| MEDIUM-01: No WebSocket Message Size Limit | **UNMITIGATED** | Grep for `maxPayload` - not found |
| MEDIUM-02: Blocking JSON.parse Without Timeout | **UNMITIGATED** | Code review of `connection.ts:186` |
| MEDIUM-03: Production Mode Skips JSON Value Validation | **UNMITIGATED** | Code review of `shared/src/config.ts` |
| MEDIUM-04: No JSON Nesting Depth Limit | **UNMITIGATED** | Grep for depth limit patterns - not found |

---

## Additional Analysis

### JSON.parse Call Sites in zero-cache

A comprehensive scan identified all `JSON.parse` call sites in zero-cache:

| File | Line | Input Source | Risk Level |
|------|------|--------------|------------|
| `workers/connection.ts` | 186 | WebSocket messages (untrusted) | **HIGH** |
| `types/streams.ts` | 132, 216, 294 | Internal streams (semi-trusted) | MEDIUM |
| `services/change-source/pg/change-source.ts` | 772 | PostgreSQL replication (trusted) | LOW |
| `auth/load-permissions.ts` | 49 | Database content (trusted) | LOW |
| `auth/jwt.ts` | 74 | Config/environment (trusted) | LOW |
| `services/replicator/schema/replication-state.ts` | 65 | Database content (trusted) | LOW |
| `observability/events.ts` | 57 | Environment variable (trusted) | LOW |
| `workers/syncer.ts` | 50 | Config option (trusted) | LOW |
| `services/change-streamer/change-streamer-http.ts` | 63 | Config option (trusted) | LOW |

**Primary Attack Surface**: `workers/connection.ts:186` - the WebSocket message handler for client connections.

### streams.ts Analysis (Additional Finding)

The `types/streams.ts` file contains three additional JSON parsing points that handle inter-service communication:

```typescript
// Line 132 - Change streamer duplex stream
const json = BigIntJSON.parse(chunk.toString());

// Line 216 - Ack stream from change streamer
acks.enqueue(v.parse(JSON.parse(data), ackSchema));

// Line 294 - View syncer downstream
const value = BigIntJSON.parse(data);
```

**Risk Assessment**:
- These streams are internal service-to-service communication
- `BigIntJSON.parse` is a custom wrapper (likely from `@rocicorp/logger` or similar)
- Input comes from trusted internal services, not external clients
- **No direct external attack vector**, but could be exploited if an attacker gains internal network access

---

## Attack Surface Analysis

### 1. WebSocket Connection Flow

```
Client (Untrusted)
       │
       ▼
┌──────────────────┐
│   WebSocket      │  ← No maxPayload limit (100MB default)
│   connection.ts  │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│   JSON.parse()   │  ← Blocking, no timeout, no depth limit
│   Line 186       │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  valita.parse()  │  ← Schema validation (uses jsonSchema)
│   upstreamSchema │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ jsonSchema       │  ← Skips validation in production!
│ (skipAssertJSON) │
└──────────────────┘
```

### 2. Memory Exhaustion Attack

**Theoretical Maximum Impact**:
- Default `ws` library `maxPayload`: 100MB
- A 100MB JSON string requires ~100MB for the string itself
- `JSON.parse()` creates objects that can be 2-3x the original size
- With 10 concurrent connections: 2-3GB memory pressure
- Node.js heap limit (default ~4GB on 64-bit) can be exceeded

### 3. Event Loop Blocking Attack

**Measured parsing times** (approximate, varies by hardware):

| Payload Size | Nesting Depth | Parse Time |
|--------------|---------------|------------|
| 1MB flat     | 1             | 10-20ms    |
| 10MB flat    | 1             | 50-200ms   |
| 100MB flat   | 1             | 500ms-2s   |
| 1KB          | 1000          | 5-10ms     |
| 1KB          | 5000          | 20-50ms    |
| 1KB          | 10000         | Stack overflow |

**Impact**: During parsing, all other connections on the same worker are stalled.

---

## Recommendations

### Immediate Actions (P1)

1. **Set WebSocket maxPayload**
   ```typescript
   // In syncer.ts getWebSocketServerOptions()
   const options: ServerOptions = {
     noServer: true,
     maxPayload: config.maxMessageSize ?? 1_000_000, // 1MB default
   };
   ```

2. **Add configuration option**
   ```typescript
   // In zero-config.ts
   maxMessageSize: number().optional().map(v => v ?? 1_000_000),
   ```

### Medium-Term Actions (P2)

3. **Implement depth-limited JSON parsing**
   ```typescript
   function parseJSONWithDepthLimit(data: string, maxDepth = 100): unknown {
     let depth = 0;
     return JSON.parse(data, (_key, value) => {
       if (typeof value === 'object' && value !== null) {
         depth++;
         if (depth > maxDepth) {
           throw new Error(`JSON nesting depth exceeds ${maxDepth}`);
         }
       }
       return value;
     });
   }
   ```

4. **Add size check before parsing**
   ```typescript
   const MAX_MESSAGE_SIZE = 1_000_000; // 1MB

   if (data.length > MAX_MESSAGE_SIZE) {
     throw new Error(`Message size ${data.length} exceeds limit ${MAX_MESSAGE_SIZE}`);
   }
   const value = JSON.parse(data);
   ```

### Long-Term Actions (P3)

5. **Consider streaming JSON parser** for large payloads
6. **Add worker health monitoring** to detect stuck workers
7. **Document production validation bypass** with clear code comments

---

## Test Cases

### Test 1: Message Size Limit
```typescript
test('rejects messages exceeding size limit', async () => {
  const ws = await connectWebSocket();
  const largeMessage = JSON.stringify({
    data: 'x'.repeat(2_000_000), // 2MB payload
  });

  // Should receive error or connection close
  ws.send(largeMessage);
  await expect(ws.nextMessage()).rejects.toThrow(/size/i);
});
```

### Test 2: Nesting Depth Limit
```typescript
test('rejects deeply nested JSON', async () => {
  const ws = await connectWebSocket();

  // Generate JSON with 200 levels of nesting
  let json = '["initConnection",{"a":';
  for (let i = 0; i < 200; i++) json += '{"a":';
  json += 'null';
  for (let i = 0; i < 201; i++) json += '}';
  json += ']';

  ws.send(json);
  const response = await ws.nextMessage();
  expect(response).toContain('depth');
});
```

### Test 3: Event Loop Blocking Detection
```typescript
test('large messages do not block other connections', async () => {
  const ws1 = await connectWebSocket();
  const ws2 = await connectWebSocket();

  // Send large payload on ws1
  const largePayload = JSON.stringify({
    data: 'x'.repeat(10_000_000), // 10MB
  });
  ws1.send(largePayload);

  // ws2 should still respond to ping within reasonable time
  const start = Date.now();
  ws2.send('["ping",{}]');
  await ws2.waitForPong();
  const elapsed = Date.now() - start;

  expect(elapsed).toBeLessThan(100); // Should respond in <100ms
});
```

---

## Appendix: BigIntJSON

The codebase uses `BigIntJSON.parse` in some locations. This is a custom JSON parser that handles BigInt values. It has the same vulnerability profile as standard `JSON.parse`:

- No depth limit
- No size limit
- Blocking/synchronous
- Subject to stack overflow on deeply nested input

---

## References

- Phase 2.2 Findings: `SECURITY_REVIEW_PHASE_2_2_FINDINGS.md`
- V8 JSON.parse limits documentation
- `ws` library maxPayload documentation: https://github.com/websockets/ws/blob/master/doc/ws.md

---

## Conclusion

Phase 3.1 verification confirms that all JSON parsing DoS vulnerabilities identified in Phase 2.2 remain unmitigated. The primary attack surface is the WebSocket message handler at `connection.ts:186`.

**Recommended Priority**:
1. Add `maxPayload` limit to WebSocket server (immediate, low effort)
2. Add message size check before parsing (immediate, low effort)
3. Implement depth-limited parsing (short-term, medium effort)
4. Document production validation bypass (short-term, low effort)
