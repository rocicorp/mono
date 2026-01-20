# Security Review Phase 4.1 - WebSocket Security

## Executive Summary

This document contains the security review findings for Phase 4.1 (WebSocket Security):
- WebSocket handoff between processes
- Connection hijacking scenarios
- Keepalive/pong mechanism
- Connection state machine

**Overall Assessment**: 2 medium-severity issues and 1 low-severity issue identified. The WebSocket architecture is generally sound, but lacks connection-level rate limiting and has potential for connection displacement attacks.

---

## Architecture Overview

### WebSocket Connection Flow

```
Client
   │
   ▼
┌─────────────────────┐
│   Main Dispatcher   │  ← HTTP Server (port 4848)
│   (ZeroDispatcher)  │
└─────────┬───────────┘
          │
          │  WebSocket Upgrade
          │  Request
          ▼
┌─────────────────────┐
│  WorkerDispatcher   │  ← Routes based on URL path
│                     │     /sync/v1/... → sync worker
└─────────┬───────────┘     /mutate/v1/... → mutator
          │
          │  IPC handoff
          │  (Socket handle)
          ▼
┌─────────────────────┐
│   Syncer Worker     │  ← Completes WS handshake
│                     │     Creates Connection
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│    Connection       │  ← Handles messages
│   (per clientID)    │     Manages state
└─────────────────────┘
```

### Key Components

| Component | File | Purpose |
|-----------|------|---------|
| ZeroDispatcher | `server/runner/zero-dispatcher.ts` | HTTP server, initial request handling |
| WorkerDispatcher | `server/worker-dispatcher.ts` | Routes WS upgrades to workers |
| Syncer | `workers/syncer.ts` | Worker process, manages connections |
| Connection | `workers/connection.ts` | Per-client connection state machine |
| websocket-handoff | `types/websocket-handoff.ts` | IPC handoff mechanism |

---

## Findings

### MEDIUM-01: Connection Displacement Attack

**Severity**: MEDIUM
**CVSS Score**: 5.3 (Medium)
**File**: `packages/zero-cache/src/workers/syncer.ts`
**Lines**: 134-140

**Vulnerability Description**:

A client can disconnect another client's active connection by connecting with the same `clientID`:

```typescript
const existing = this.#connections.get(clientID);
if (existing) {
  this.#lc.debug?.(
    `client ${clientID} already connected, closing existing connection`,
  );
  existing.close(`replaced by ${params.wsID}`);  // Closes legitimate user
}
```

**Attack Scenario**:
1. Legitimate user "Alice" connects with `clientID=alice-device-1`
2. Attacker discovers or guesses the clientID
3. Attacker connects with same `clientID=alice-device-1`
4. Alice's connection is forcibly closed
5. Alice's queries stop updating until she reconnects

**Impact**:
- Denial of service to specific users
- Can be used to force users offline repeatedly
- clientID is visible in URL query parameters (not authenticated before displacement check)

**Risk Assessment**:
- ClientIDs are typically UUIDs, making guessing difficult
- However, if clientID is predictable or logged/exposed, attack is trivial
- JWT validation happens AFTER the connection displacement

**Recommendation**:

1. Validate JWT before allowing connection displacement:
```typescript
const existing = this.#connections.get(clientID);

// Verify auth BEFORE displacement
let decodedToken: JWTPayload | undefined;
if (auth && tokenOptions.length > 0) {
  decodedToken = await verifyToken(this.#config.auth, auth, { subject: userID });
}

if (existing) {
  // Only allow displacement if new connection is authenticated
  // and belongs to same user
  if (!decodedToken || decodedToken.sub !== existing.userID) {
    sendError(..., { kind: ErrorKind.Unauthorized, ... });
    ws.close(3000, 'Cannot displace connection');
    return;
  }
  existing.close(`replaced by ${params.wsID}`);
}
```

2. Add rate limiting on connection attempts per clientID.

---

### MEDIUM-02: No Connection Rate Limiting

**Severity**: MEDIUM
**CVSS Score**: 5.3 (Medium)
**Files**:
- `packages/zero-cache/src/workers/syncer.ts`
- `packages/zero-cache/src/server/worker-dispatcher.ts`

**Vulnerability Description**:

While mutation rate limiting exists (`perUserMutationLimit`), there is no rate limiting on WebSocket connection attempts. An attacker can:

1. Repeatedly connect/disconnect to exhaust server resources
2. Send many connection attempts to different clientIDs
3. Trigger expensive operations (JWT validation, connection setup)

**Impact**:
- CPU exhaustion from JWT validation
- Memory pressure from connection state allocation
- Worker thread starvation
- Potential service degradation for legitimate users

**Recommendation**:

1. Add connection rate limiting per IP:
```typescript
const connectionLimiter = new Map<string, SlidingWindowLimiter>();

function getConnectionLimiter(ip: string): SlidingWindowLimiter {
  let limiter = connectionLimiter.get(ip);
  if (!limiter) {
    limiter = new SlidingWindowLimiter(60_000, 100); // 100 connections/minute
    connectionLimiter.set(ip, limiter);
  }
  return limiter;
}

// In dispatcher:
const clientIP = getClientIP(request);
if (!getConnectionLimiter(clientIP).canDo()) {
  throw new Error('Connection rate limit exceeded');
}
```

2. Add per-clientGroupID connection limits to prevent resource exhaustion.

---

### LOW-01: WebSocket Compression DoS Potential

**Severity**: LOW
**CVSS Score**: 3.7 (Low)
**File**: `packages/zero-cache/src/workers/syncer.ts`
**Lines**: 40-63

**Vulnerability Description**:

WebSocket compression options are configurable but default to false. If enabled with default options, the server may be vulnerable to compression-related DoS:

```typescript
function getWebSocketServerOptions(config: ZeroConfig): ServerOptions {
  const options: ServerOptions = {
    noServer: true,
  };

  if (config.websocketCompression) {
    options.perMessageDeflate = true;  // Default ws library settings

    if (config.websocketCompressionOptions) {
      // User can provide custom options
      const compressionOptions = JSON.parse(
        config.websocketCompressionOptions,
      );
      options.perMessageDeflate = compressionOptions;
    }
  }

  return options;
}
```

**Impact**:
- CPU exhaustion from compression ratio attacks (highly compressible payloads)
- Memory exhaustion from decompression bombs
- Default `ws` library settings may not be optimally tuned

**Risk Assessment**:
- Compression is disabled by default (mitigates risk)
- User must explicitly enable via `ZERO_WEBSOCKET_COMPRESSION=true`
- Custom options allow tuning but require expertise

**Recommendation**:
1. Document compression security considerations
2. Add recommended compression settings in config documentation
3. Consider adding sensible limits even when compression is enabled:
```typescript
const SAFE_COMPRESSION_DEFAULTS = {
  threshold: 1024,  // Only compress messages > 1KB
  zlibDeflateOptions: {
    level: 6,       // Balanced compression (not max)
    memLevel: 7,    // Limit memory usage
  },
  clientMaxWindowBits: 13,  // Limit window size
  serverMaxWindowBits: 13,
};
```

---

## Positive Findings

### WebSocket Handoff Mechanism - SECURE

**Status**: SECURE
**File**: `packages/zero-cache/src/types/websocket-handoff.ts`

The IPC handoff mechanism is well-implemented:

1. **Uses Node.js structured clone**: `serialization: 'advanced'` ensures proper serialization
2. **Socket handles transferred properly**: Via `SendHandle` parameter
3. **Error handling**: Errors during handoff result in proper WebSocket error closure
4. **No race conditions**: Socket closed during handoff is detected and handled

```typescript
// Per Node.js docs requirement - checks socket validity
if (!socket) {
  lc.warn?.('websocket closed during handoff');
  return;
}
```

### Keepalive/Pong Mechanism - SECURE

**Status**: SECURE
**Files**:
- `packages/zero-cache/src/workers/connection.ts`
- `packages/zero-cache/src/types/ws.ts`

The keepalive mechanism is robust:

1. **Bidirectional heartbeats**: Both client and server send heartbeats
2. **Back-pressure aware**: Pongs sent even if message queue is backed up
3. **Grace period**: 6 second interval with buffer for latency
4. **Clean termination**: Dead connections are terminated via `ws.terminate()`

```typescript
// Server sends pong if no downstream message in 6 seconds
const DOWNSTREAM_MSG_INTERVAL_MS = 6_000;

#maybeSendPong = () => {
  if (Date.now() - this.#lastDownstreamMsgTime > DOWNSTREAM_MSG_INTERVAL_MS) {
    this.#lc.debug?.('manually sending pong');
    this.send(['pong', {}], 'ignore-backpressure');
  }
};
```

### Connection State Machine - ADEQUATE

**Status**: ADEQUATE
**File**: `packages/zero-cache/src/workers/connection.ts`

The state machine is reasonably safe:

1. **Closed flag**: Prevents double-close and message processing after close
2. **Protocol version check**: Early validation prevents incompatible clients
3. **Event listener cleanup**: Properly removes listeners on close
4. **Stream cancellation**: Outbound streams are cancelled on close

```typescript
close(reason: string, ...args: unknown[]) {
  if (this.#closed) {
    return;  // Prevent double-close
  }
  this.#closed = true;
  // ... cleanup
}

#handleMessage = async (event: {data: Data}) => {
  if (this.#closed) {
    this.#lc.debug?.('Ignoring message received after closed', data);
    return;  // Prevent processing after close
  }
  // ...
}
```

### Worker Distribution - SECURE

**Status**: SECURE
**File**: `packages/zero-cache/src/server/worker-dispatcher.ts`

Client group to worker assignment is deterministic and consistent:

```typescript
// Include TaskID for distribution diversity across server instances
const syncer = h32(taskID + '/' + clientGroupID) % syncers.length;
```

This ensures:
- Same clientGroupID always goes to same worker (connection affinity)
- Different server instances distribute differently (load balancing on failover)
- No race conditions in worker selection

---

## Remediation Priority

| Finding | Severity | Effort | Priority |
|---------|----------|--------|----------|
| MEDIUM-01: Connection displacement | Medium | Medium | P1 |
| MEDIUM-02: No connection rate limiting | Medium | Medium | P1 |
| LOW-01: Compression DoS potential | Low | Low | P3 |

---

## Recommended Test Cases

### For MEDIUM-01 (Connection Displacement):
```typescript
test('cannot displace connection without valid auth', async () => {
  // Connect as legitimate user
  const ws1 = await connect({ clientID: 'client1', auth: validToken });
  expect(ws1.readyState).toBe(WebSocket.OPEN);

  // Try to displace without auth
  const ws2 = await connect({ clientID: 'client1', auth: undefined });
  expect(ws2.readyState).toBe(WebSocket.CLOSED);
  expect(ws1.readyState).toBe(WebSocket.OPEN);  // Original still connected
});

test('cannot displace connection with different user', async () => {
  const ws1 = await connect({ clientID: 'client1', auth: tokenForUser('alice') });
  const ws2 = await connect({ clientID: 'client1', auth: tokenForUser('bob') });

  expect(ws2.readyState).toBe(WebSocket.CLOSED);  // Bob rejected
  expect(ws1.readyState).toBe(WebSocket.OPEN);    // Alice still connected
});
```

### For MEDIUM-02 (Rate Limiting):
```typescript
test('rate limits connection attempts from same IP', async () => {
  const attempts = [];
  for (let i = 0; i < 150; i++) {
    attempts.push(connect({ clientID: `client-${i}` }));
  }

  const results = await Promise.allSettled(attempts);
  const rejected = results.filter(r => r.status === 'rejected');

  expect(rejected.length).toBeGreaterThan(0);  // Some should be rate limited
});
```

---

## Files Reviewed

| File | Purpose | Findings |
|------|---------|----------|
| `packages/zero-cache/src/types/websocket-handoff.ts` | IPC handoff | Secure |
| `packages/zero-cache/src/workers/syncer.ts` | Worker process | Connection displacement |
| `packages/zero-cache/src/workers/connection.ts` | Connection state machine | Adequate |
| `packages/zero-cache/src/workers/connect-params.ts` | Parameter extraction | No issues |
| `packages/zero-cache/src/server/worker-dispatcher.ts` | Request routing | No rate limiting |
| `packages/zero-cache/src/types/ws.ts` | WebSocket utilities | Secure |
| `packages/zero-cache/src/types/processes.ts` | IPC primitives | Secure |
| `packages/zero-cache/src/types/http.ts` | HTTP message subset | Secure |

---

## Appendix: Connection Lifecycle

```
┌─────────────────┐
│  WS Upgrade     │
│  Request        │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Parse Params   │ ← clientID, clientGroupID from URL
│  (connect-params)│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Handoff to     │ ← IPC to worker based on clientGroupID hash
│  Worker         │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Check Existing │ ← **VULNERABILITY**: Displacement before auth
│  Connection     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Verify JWT     │ ← Token validation (if configured)
│  (if auth)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Create         │ ← Connection, ViewSyncer, Mutagen
│  Connection     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Send           │ ← ["connected", {wsid, timestamp}]
│  "connected"    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Message Loop   │ ← ping/pong, queries, mutations
│                 │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Close          │ ← Cleanup streams, remove from map
│                 │
└─────────────────┘
```
