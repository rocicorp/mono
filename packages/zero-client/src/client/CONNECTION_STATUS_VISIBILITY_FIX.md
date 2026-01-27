# Connection Status Flash on Tab Visibility Change

## Problem

When a Zero app goes into a hidden tab, after ~5 seconds Zero disconnects. When the tab refocuses, Zero reconnects. However, if the app uses the connection status API, users see a flash of the `disconnected` state for about a second before it jumps to `connected`.

Users expect to see `connecting` while the reconnection is in progress, not `disconnected`.

## Root Cause

The issue is in `connection-manager.ts` lines 219-224:

```typescript
// we cannot intentionally transition from disconnected to connecting
// disconnected can transition to connected on successful connection
// or a terminal state
if (this.#state.name === ConnectionStatus.Disconnected) {
  return {nextStatePromise: this.#nextStatePromise()};
}
```

### Timeline of what happens:

1. Tab is `Connected`
2. Tab hidden → after 5 seconds (`hiddenTabDisconnectDelay`), `#disconnect(Hidden)` is called
3. `Hidden` error → `NO_STATUS_TRANSITION` → calls `connecting()`, state becomes `Connecting`
4. Tab remains hidden for > 60 seconds (`disconnectTimeoutMs`)
5. `ConnectionManager`'s timeout interval fires, transitions `Connecting` → `Disconnected`
6. Tab becomes visible → `waitForVisible()` resolves → `#connect()` is called
7. `#connect()` calls `connectionManager.connecting()` **but this is a no-op when in `Disconnected` state**
8. State stays `Disconnected` during the entire WebSocket handshake (~1 second)
9. Only when socket connects do we jump directly to `Connected`

### Why it was designed this way:

The `Disconnected` state was meant to signal "we've been trying for 60 seconds and failed" - a warning to the user. The thinking was: if you couldn't connect for a full minute, bouncing back to `Connecting` might be misleading during prolonged connectivity issues.

The problem is this logic doesn't account for the hidden-tab case. When a tab becomes visible after being hidden, showing `Disconnected` → `Connected` is confusing - the user expects `Connecting` while reconnection is in progress. The tab wasn't "failing to connect", it intentionally stopped.

## Proposed Solution

Add a flag to `connecting()` that allows forcing the transition from `Disconnected` when the tab becomes visible:

### In `connection-manager.ts`:

```typescript
connecting(reason?: ZeroError, options?: {fromVisibilityChange?: boolean}): {
  nextStatePromise: Promise<ConnectionManagerState>;
} {
  // ... existing closed check ...

  // Allow transition from disconnected only on visibility change
  if (this.#state.name === ConnectionStatus.Disconnected) {
    if (!options?.fromVisibilityChange) {
      return {nextStatePromise: this.#nextStatePromise()};
    }
    // Reset the connecting start time for a fresh timeout window
    this.#connectingStartedAt = undefined;
    // Fall through to create new Connecting state
  }

  // ... rest of existing logic (starting from line 226)
}
```

### In `zero.ts` run loop (~line 1939):

Before calling `#connect()`, check if we're disconnected and force the transition:

```typescript
// If we're disconnected and just became visible, force transition to connecting
if (this.#connectionManager.is(ConnectionStatus.Disconnected)) {
  this.#connectionManager.connecting(undefined, {fromVisibilityChange: true});
}

await this.#connect(lc, additionalConnectParams);
```

## Why this approach

1. **Minimal change** - just adds an opt-in flag
2. **Preserves existing behavior** - genuine connectivity failures (repeated retries) stay in `Disconnected`
3. **Targeted fix** - only the visibility-change path opts into the new behavior
4. **Fresh timeout window** - resets `#connectingStartedAt` so the user gets a fresh 60 seconds of `Connecting` before potentially going back to `Disconnected`

## Files to modify

- `packages/zero-client/src/client/connection-manager.ts`
- `packages/zero-client/src/client/zero.ts`
- Tests in `packages/zero-client/src/client/connection-manager.test.ts` and `packages/zero-client/src/client/zero.test.ts`
