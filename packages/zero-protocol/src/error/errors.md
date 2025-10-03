# Zero Error Handling Deep Dive

This document surveys how errors move from zero-cache to zero-client, how they surface through `useQuery` and `zero.mutate`, and where the current handling leaves gaps. The focus is on named/custom queries and custom mutators, but connection-level behaviour that affects all clients is included for context.

## Query Error Paths (named & legacy)

| Scenario | Zero-cache behaviour | Client message | Zero-client handling | Developer exposure |
| --- | --- | --- | --- | --- |
| Custom query transform returns `{error: 'app'}` from API server | `CustomQueryTransformer` keeps response as-is and sends `transformError` to interested clients (`packages/zero-cache/src/custom-queries/transform-query.ts:95-156`) | `['transformError', [ErroredQuery…]]` | `QueryManager.handleTransformErrors` invokes registered callbacks with the error (`packages/zero-client/src/client/query-manager.ts:208-215`) and the affected view publishes a `QueryResultDetails` error | `useQuery` snapshot carries `{type: 'error', error: {type: 'app', queryName, details}}` (`packages/zero-react/src/use-query.tsx:184-266`); consumer can call the provided `refetch` |
| Custom query transform receives non-200 HTTP status | Transformer maps every outstanding request to `{error: 'http', status, details}` before sending (`packages/zero-cache/src/custom-queries/transform-query.ts:121-129`) | `['transformError', …]` | Same as above | `useQuery` error has `type: 'http'` with status/details; data remains last optimistic state |
| Custom query transform throws (network error, JSON parse, etc.) | For network/timeout the transformer emits `{error: 'zero', details}`; schema parse exceptions bubble up, causing view-syncer to `fail` all clients with `ErrorKind.Internal` (`packages/zero-cache/src/custom-queries/transform-query.ts:109-118`, `packages/zero-cache/src/services/view-syncer/view-syncer.ts:400-415`) | `['transformError', …]` or an `'error'` message with `kind: Internal` | `'zero'` errors become `type: 'app'` in `useQuery`; parser failures close the socket and trigger reconnect via `#handleErrorMessage` | Developers cannot distinguish Zero-internal issues from app errors because `'zero'` errors are normalised to `type: 'app'`; connection-level errors require monitoring `onError` |
| Custom query transform hits 401 (auth invalid) | `fetchFromAPIServer` throws `ErrorForClient {AuthInvalidated}` (`packages/zero-cache/src/custom-queries/transform-query.ts:100-111`) → connection closes | `['error', {kind: 'AuthInvalidated', …}]` | `Zero.#handleErrorMessage` disconnects and rejects the socket (`packages/zero-client/src/client/zero.ts:1113-1154`); the run loop catches the auth `ServerError`, flips `needsReauth`, and refreshes the token before reconnecting (`packages/zero-client/src/client/zero.ts:1688-1760`) | `useQuery` views go unknown until reconnect; only `onError`/logs record the cause |
| Legacy query pipeline error (permissions, schema mismatch) | Raises `ErrorForClient` (e.g. in `packages/zero-cache/src/services/view-syncer/view-syncer.ts:1991-2053`) | `['error', {...}]` | Client disconnects, often followed by reload/DB reset depending on `kind` (`packages/zero-client/src/client/zero.ts:1119-1154`) | No per-query error; developer must watch `onError` or update callbacks |

### Observations

- Named queries are the only path that yield structured per-query errors. Legacy queries either succeed or tear down the connection.
- Zero-internal failures during custom query transforms are exposed as `type: 'app'`, making it impossible to distinguish user faults from platform issues.
- Retry semantics rely on the `refetch` function in the snapshot; there is no automatic exponential backoff beyond reconnect cycles.

## Mutation Error Paths (custom mutators unless noted)

| Scenario | Zero-cache behaviour | Client message | Zero-client handling | Developer exposure |
| --- | --- | --- | --- | --- |
| API responds with `mutation.result.data` | `PushWorker` groups responses and does not emit a downstream message (`packages/zero-cache/src/services/mutagen/pusher.ts:341-377`) | Mutation result is written to the `mutations` feed; no extra message | `MutationTracker` resolves the tracked promise when the result row arrives (`packages/zero-client/src/client/mutation-tracker.ts:120-137`, `287-326`) | `MutatorResult.server` resolves with data (`packages/zero-client/src/client/custom.ts:45-92`) |
| API responds with `mutation.result.error: 'app'` | Logged and left in the response list (`packages/zero-cache/src/services/mutagen/pusher.ts:341-357`) | Result row contains the error | `MutationTracker.#processMutationError` rejects the server promise with the typed error (`packages/zero-client/src/client/mutation-tracker.ts:252-285`) | Developer must await `mutate(...).server` to see the rejection; awaiting the mutator directly only yields optimistic completion |
| API responds with `mutation.result.error: 'oooMutation'` | Marks failure and terminates the connection with `ErrorKind.InvalidPush` (`packages/zero-cache/src/services/mutagen/pusher.ts:349-376`) | Connection failure -> `['error', {kind: 'InvalidPush'}]` | Server promise rejects; connection closes | User sees rejection *and* reconnect churn; no targeted guidance |
| API responds with `mutation.result.error: 'alreadyProcessed'` | Duplicate acknowledgements ignored; originator still receives rejection (`packages/zero-client/src/client/mutation-tracker.ts:252-285`) | None | Server promise rejects (unless mutation belonged to another tab) | App must handle idempotency manually |
| API returns HTTP error (e.g. 500) | `PushWorker` pushes `['pushResponse', {error: 'http', status, details, mutationIDs}]` (`packages/zero-cache/src/services/mutagen/pusher.ts:300-338`) | `['pushResponse', {...}]` | `MutationTracker.processPushResponse` only logs at error level (`packages/zero-client/src/client/mutation-tracker.ts:155-163`) and leaves mutations outstanding | Developer has no programmatic signal; needs `onError` hook to surface status to the UI |
| API request throws (timeout/network) | Emits `['pushResponse', {error: 'zeroPusher', details}]` (`packages/zero-cache/src/services/mutagen/pusher.ts:443-463`) | Same as above | Same as above | Same blind spot as HTTP failures; mutations retry silently |
| API responds with `error: 'unsupportedPushVersion'`/`'unsupportedSchemaVersion'` | Connection is failed with `ErrorKind.InvalidPush` (`packages/zero-cache/src/services/mutagen/pusher.ts:317-336`) | `['error', {kind: 'InvalidPush'}]` | Client disconnects; outstanding mutations stay pending until reconnect | Only global error handling; no per-mutation feedback |
| API responds with `error: 'forClient'` (explicit `ErrorForClient`) | Connection is failed with provided payload (`packages/zero-cache/src/services/mutagen/pusher.ts:326-327`) | `['error', errorBody]` | Client disconnects; outstanding mutations retried after reconnect | App must listen to `onError` to surface the cause |
| CRUD mutation hits business logic error | `Mutagen` reruns mutation in error mode and returns `[ErrorKind.MutationFailed, message]` (`packages/zero-cache/src/services/mutagen/mutagen.ts:240-284`) | `['error', {kind: 'MutationFailed', message}]` | `Zero.#handleErrorMessage` treats as fatal and disconnects (`packages/zero-client/src/client/zero.ts:1119-1154`); there is no per-mutation rejection | Developers have no hook—the connection resets and optimistic state rewinds |
| Mutation rate limit exceeded | `Mutagen` returns `[MutationRateLimited, 'Rate limit exceeded']` (`packages/zero-cache/src/services/mutagen/mutagen.ts:171-188`, `processMutation`) | `['error', {kind: 'MutationRateLimited', message}]` | `Zero.#handleErrorMessage` logs, clears `lastMutationID` so the mutation will be retried, but keeps the socket alive (`packages/zero-client/src/client/zero.ts:1119-1125`) | Only surfaced via `onError`; outstanding mutation silently requeues |

### Observations

- Custom mutators deliver typed application errors *only* through the `MutatorResult.server` promise. Applications that `await zero.mutate.foo()` never see server failures.
- HTTP/network failures during push merely log an error; there is no rejection or state change for the affected mutations, so UI cannot notify users.
- CRUD error handling is especially rough: a single mutation failure disconnects the client, with no structured report back to application code.

## Connection-Level ErrorKinds

| ErrorKind | Typical source | Zero-client response | Default developer signal |
| --- | --- | --- | --- |
| `AuthInvalidated` / `Unauthorized` | 401 from mutate/query fetch (`fetchFromAPIServer`) or token mismatch (`packages/zero-cache/src/services/view-syncer/view-syncer.ts:1991-2053`) | Disconnect, mark `needsReauth`, fetch new token (`packages/zero-client/src/client/zero.ts:1613-1750`) | `onError` + reconnect side-effects; queries go stale during recovery |
| `ClientNotFound` | Missing CVR / client state (`packages/zero-cache/src/services/view-syncer/view-syncer.ts:377-414`) | Disable client group, invoke `onClientStateNotFound` then reload/clear IDB (`packages/zero-client/src/client/zero.ts:1137-1154`) | Requires user-supplied handler; default reload |
| `InvalidConnectionRequest{BaseCookie,LastMutationID}` | BaseCookie (client ahead): `checkClientAndCVRVersions` rejects the handshake when the client's cookie is newer than the CVR (zero-cache has fallen behind or restarted without a snapshot, see `packages/zero-cache/src/services/view-syncer/view-syncer.ts:1988`).<br>LastMutationID (client behind): reserved for the (planned) sanity check that compares the client's declared last mutation against `zero.clients.lastMutationID`; the server does **not** emit this variant yet, but once wired it will mean zero-cache has processed more mutations for the client than the client remembers, so the client must discard its local state and catch up from scratch. | Drop local database then schedule reload (`packages/zero-client/src/client/zero.ts:1149-1154`) | Forced reload; no opportunity for app UI; console shows the server-ahead explanation stored in `serverAheadReloadReason` |
| `InvalidPush` | Emitted from several guardrails:<br>- Websocket handler rejects pushes whose `clientGroupID` doesn't match the connection (`packages/zero-cache/src/workers/syncer-ws-message-handler.ts:93-107`).<br>- Pusher fails the connection if the API reports `unsupportedPushVersion` / `unsupportedSchemaVersion` or flags an out-of-order mutation (`packages/zero-cache/src/services/mutagen/pusher.ts:304-377`).<br>- Mutagen rethrows custom mutator errors marked as `InvalidPush`, typically when the server has already advanced `lastMutationID` for that client (`packages/zero-cache/src/services/mutagen/mutagen.ts:256-280`). | Disconnect and rely on reconnect | Only via `onError`; individual mutation context lost |
| `MutationFailed` | CRUD mutation application error (`packages/zero-cache/src/services/mutagen/mutagen.ts:240-284`) | Disconnect | No structured signal beyond global error |
| `MutationRateLimited` | Per-user limiter in mutagen (`packages/zero-cache/src/services/mutagen/mutagen.ts:171-188`) | Log + retry later, connection stays up (`packages/zero-client/src/client/zero.ts:1119-1125`) | Requires `onError`; app cannot distinguish from transient retry |
| `VersionNotSupported` / `SchemaVersionNotSupported` | Protocol mismatch (`packages/zero-cache/src/workers/connection.ts:78-120`, `packages/zero-cache/src/services/view-syncer/view-syncer.ts:377-414`) | Trigger `onUpdateNeeded` callback (`packages/zero-client/src/client/zero.ts:1137-1144`) | Default behaviour is to reload |
| `Rebalance` / `Rehome` / `ServerOverloaded` | Shared backoff channel. Code using the `BackoffBody` schema can set `minBackoffMs`/`maxBackoffMs` and optional `reconnectParams` to steer the next connect attempt (`packages/zero-protocol/src/error.ts:35-45`). `Rehome` is currently emitted when CVR ownership moves to another worker so the client reconnects immediately against the new owner (`packages/zero-cache/src/services/view-syncer/cvr-store.ts:949-955`). `ServerOverloaded` is used to stretch the backoff window when the server asks clients to slow down (see `packages/zero-client/src/client/zero.test.ts:223-242` for expected payload). `Rebalance` is reserved for rebucketting client groups and follows the same payload shape. | Run-loop backs off, merges the hinted delays, and forwards any reconnect params into the next `/connect` URL (`packages/zero-client/src/client/zero.ts:1723-1768`) | Only visible via logs; apps must inspect `onError` if they want to surface the reason |
| `Internal` | Unhandled exceptions server-side | Disconnect | Only `onError`/console |

## Surface Areas for Application Code

### `useQuery`
- Every view returns `[data, QueryResultDetails]`. When a named query errors, `QueryResultDetails` becomes `{type: 'error', error: {type: 'app'|'http', queryName, details}, refetch}` (`packages/zero-react/src/use-query.tsx:184-267`).
- Zero’s client normalises Zero-internal errors to `type: 'app'`, so applications cannot differentiate network faults from server-side validation failures.
- Legacy queries never emit `QueryResultDetails.error`; instead, fatal issues collapse the WebSocket connection.
- Re-fetching simply re-creates the view, causing another round-trip. There is no built-in cooldown or retry policy beyond what the app implements.

### `zero.mutate`
- Custom mutators return `{client, server}` promises (`packages/zero-client/src/client/custom.ts:45-92`). The `client` promise resolves when optimistic work finishes; the `server` promise resolves/rejects when zero-cache observes the mutation result.
- `MutatorResult` still implements `.then` for backwards compatibility (with a deprecation warning), so `await zero.mutate.foo()` only observes optimistic completion. Surface server failures by awaiting `.server` (or handling rejections via `.catch`).
- Server-side application errors (`mutation.result.error: 'app'`) reject the `server` promise. Platform errors during push (`pushResponse.error: 'http' | 'zeroPusher'`) only trigger an `onError` log—mutations remain pending with no rejection.
- CRUD mutators lack a structured server promise. When a CRUD mutation fails, the client simply disconnects, forcing a reconnect with no programmatic hook.

### `onError`
- The Zero constructor wraps the log sink so any `error`-level log is forwarded to `options.onError` when provided (`packages/zero-client/src/client/zero.ts:498-507`). This is currently the only way to capture push HTTP failures, mutation rate limiting, and most connection errors without parsing console output.

## Connection Reset Triggers

| Trigger | Source | Current behaviour | Rationale today |
| --- | --- | --- | --- |
| AuthInvalidated / Unauthorized | 401 from mutate/query fetch (`fetchFromAPIServer`) or token mismatch during handshake (`packages/zero-cache/src/services/view-syncer/view-syncer.ts:1991-2053`) | `Zero.#handleErrorMessage` disconnects and rejects the socket (`packages/zero-client/src/client/zero.ts:1113-1154`); the run loop sets `needsReauth`, refreshes the token, and reconnects (`packages/zero-client/src/client/zero.ts:1688-1760`) | Forces a clean handshake because the existing JWT is invalid |
| InvalidPush (ooo mutation, unsupported push/schema version, explicit `forClient`) | Emitted by pusher when the API server rejects a mutation batch (`packages/zero-cache/src/services/mutagen/pusher.ts:300-377`) | Connection fails with `ErrorKind.InvalidPush`; client disconnects and retries after reconnect | Avoids applying out-of-order or incompatible mutations but loses per-mutation context |
| MutationFailed (CRUD mutators) | `Mutagen` replays mutation in error mode then throws `ErrorKind.MutationFailed` (`packages/zero-cache/src/services/mutagen/mutagen.ts:240-284`) | `Zero.#handleErrorMessage` treats it as fatal and disconnects | Legacy CRUD path can only advance LMID by reconnecting |
| SchemaVersionNotSupported / ClientNotFound / InvalidConnectionRequest* | View-syncer detects schema drift or inconsistent LMID (`packages/zero-cache/src/services/view-syncer/view-syncer.ts:377-414`) | Client disables the group, drops IDB, and reloads via `onUpdateNeeded`/`onClientStateNotFound` | Guarantees client restarts from a compatible snapshot |
| ErrorKind.Internal (transform parse failure, server crash) | Thrown anywhere inside view-syncer/pusher and sent as `['error', {...}]` | `Zero` disconnects and run loop retries with backoff | Defensive posture for unknown faults, but the callsite loses the original context |

### How this should evolve
- Convert mutation- and query-scoped issues (InvalidPush tied to a single batch, MutationFailed, `{error:'zero'}` transform failures) into per-callsite rejections while keeping a global notification so teams can display inline errors and still log globally. Connection resets should be reserved for genuine protocol corruption, not business-logic failures.
- When a reset is unavoidable (schema drift, deleted client state), surface explicit reason objects to both `onError` and the affected operations so UI can differentiate "please reload" from transient outages.
- Provide configuration or feature flags for legacy paths (CRUD) to opt into the new behaviour so apps can migrate without rewriting mutations yet still avoid tear-downs for recoverable issues.

## Critique

- **Developers need layered error surfaces:** Most app teams want to show end users contextual errors (e.g. inline next to a failed query or mutation) while also logging or toasting global faults. Currently only named queries surface callsite errors; everything else collapses into the global `onError` sink or a disconnect. Providing both a per-callsite signal *and* a global callback for every class of error would let teams build consistent UX (e.g. optimistic mutation toast plus background logging) without reverse-engineering connection failures.
- **Large class of errors are effectively unhandled:** Push HTTP/timeout failures, network exceptions inside `fetchFromAPIServer`, and mutation rate limiting only write to the log sink. CRUD mutation failures and schema mismatches silently trigger reconnects without any rejection at the mutation callsite. These scenarios leave product code unaware that user intent failed and prevent retry/rollback flows.
- **Platform vs application fault semantics blur together:** `{error: 'zero'}` transform responses are coerced to `type: 'app'`, and `ErrorKind.MutationFailed` tears down the socket the same way as an authorization issue. Developers cannot tell whether to fix their business logic, refresh credentials, or open an incident.
- **Connection resets are doing too much work:** Using reconnect/reload as the primary recovery mechanism erases optimistic UI state, forces broad retry cascades, and hides the original failure from the component that triggered it.
- **Backwards compatibility shim still trips people up:** Because `.then` is still implemented on `MutatorResult`, it is easy to miss server-side errors unless teams migrate fully to `.server`/`.client`, so many apps will continue to swallow push results.

**Opportunities for improvement**
- Emit structured events for every failure class with both global and scoped handlers: e.g. push failures reject the originating `MutatorResult.server` *and* invoke `onError`, connection faults provide a typed payload beyond `ErrorKind`.
- Preserve the distinction between platform and application faults when propagating errors so callsites can choose whether to show inline messaging, retry, or escalate globally.
- Provide escape hatches for legacy/CRUD paths (even if deprecated) so failures don't only manifest as reconnects; e.g. optional callbacks or feature flags that surface errors without teardown.

Closing these gaps would give application developers predictable hooks to inform users, to retry selectively, and to escalate only the errors that truly require global attention.
