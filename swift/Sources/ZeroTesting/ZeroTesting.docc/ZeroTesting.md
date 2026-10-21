# ``ZeroTesting``

Check your Swift queries and mutators against the TypeScript ones, and
test code that uses a client without a server.

## Overview

### The drift harness

You write the Swift queries and mutators by hand, so they can drift from
the TypeScript ones the server runs. `zero-swift drift record` runs the
TypeScript definitions on sample arguments and contexts and writes a
vectors file. `Drift` checks the Swift definitions against those vectors:

```swift
@Test @MainActor func matchesTheTSQueriesAndMutators() throws {
  let vectors = try String(contentsOf: vectorsURL, encoding: .utf8)
  let failures = try Drift.failures(
    vectors: vectors, schema: .app, queries: Queries.all, mutators: Mutators.all)
  #expect(failures == [])
}
```

The test fails, listing each difference, when:

- a name is in one registry but not the other;
- a sample's arguments don't round-trip through the Swift argument type;
- a query builds a different AST;
- a mutator, run on the sample's rows, changes different rows or ends
  differently.

### A client without a server

`ManualScheduler` is a clock that moves only when the test advances it.
`MockSocketFactory` hands the client sockets that the test drives as the
server:

```swift
let scheduler = ManualScheduler()
let sockets = MockSocketFactory()
let zero = try Zero(
  schema: .app, context: auth, options: ZeroOptions(cacheURL: "http://zero.test"),
  scheduler: scheduler, socketFactory: sockets)

scheduler.runDueTimers()                 // the client opens its socket
let socket = try #require(sockets.sockets.first)
socket.receive(text: #"["connected",{"wsid":"w1"}]"#)
#expect(zero.connection.state == .connected)
```

`RecordingLogSink` keeps the client's log messages for a test to check.

### A server in the app

For previews, demos, and UI tests that need synced data on real time,
`LoopbackServer` plays a server in the app's process: it sends the rows it
holds, confirms every query, and syncs rows added later, as another user's
change would.

```swift
let server = LoopbackServer()
server.put("message", ["id": "m1", "body": "Hi", "createdAt": 1])
let zero = try Zero(
  schema: .app, options: ZeroOptions(cacheURL: "http://loopback"),
  socketFactory: server)
```

`ZeroTesting` re-exports `Zero`.

## Topics

### Drift

- ``Drift``

### Test doubles

- ``ManualScheduler``
- ``MockSocketFactory``
- ``MockSocket``
- ``RecordingLogSink``
- ``LoopbackServer``
