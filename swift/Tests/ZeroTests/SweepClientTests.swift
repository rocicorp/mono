import Testing
import ZeroTesting

@testable import Zero

/// Edge cases of the client engine checked against the JS client
/// (`packages/zero-client/src/client`): pokes, pushes, the run loop, and
/// views and preloads.
@Suite @MainActor struct SweepClientTests {
  static let all = defineQuery("issues.all") { () in zql.issue.orderBy(\.id, .asc) }
  static let create = defineMutator("issue.create") { (tx, id: String) in
    try tx.mutate.issue.insert([
      "id": .string(id), "title": "", "description": "", "closed": false, "ownerId": .null,
      "createdAt": 1,
    ])
  }
  static let createThenThrow = defineMutator("issue.createThenThrow") { (tx, id: String) in
    try tx.mutate.issue.insert([
      "id": .string(id), "title": "", "description": "", "closed": false, "ownerId": .null,
      "createdAt": 1,
    ])
    throw ApplicationError("midway")
  }

  let scheduler = ManualScheduler(now: 1_700_000_000_000)
  let sockets = MockSocketFactory()
  let log = RecordingLogSink()

  func makeZero() throws -> Zero<Void> {
    var options = ZeroOptions(
      cacheURL: "http://zero.test", userID: "u", storageKey: randomID(), storage: .memory)
    options.logSink = log
    return try Zero(
      schema: .test, mutators: [Self.create, Self.createThenThrow], options: options,
      scheduler: scheduler, socketFactory: sockets)
  }

  /// Opens the newest socket and acknowledges it.
  func connect() throws -> MockSocket {
    scheduler.runDueTimers()
    let socket = try #require(sockets.sockets.last)
    socket.receive(text: #"["connected",{"wsid":"w"}]"#)
    scheduler.runDueTimers()
    return socket
  }

  func poke(
    _ socket: MockSocket, _ id: String, from base: String?, to cookie: String, _ part: String?
  ) {
    let base = base.map { "\"\($0)\"" } ?? "null"
    socket.receive(text: #"["pokeStart",{"pokeID":"\#(id)","baseCookie":\#(base)}]"#)
    if let part { socket.receive(text: #"["pokePart",{"pokeID":"\#(id)",\#(part)}]"#) }
    socket.receive(text: #"["pokeEnd",{"pokeID":"\#(id)","cookie":"\#(cookie)"}]"#)
    scheduler.runDueTimers()
  }

  static let hash = QueryHash.hashOfNameAndArgs("issues.all", [])
  static let gotAll = #""gotQueriesPatch":[{"op":"put","hash":"\#(hash)"}]"#
  static func row(_ id: String) -> String {
    #"{"op":"put","tableName":"issues","value":{"id":"\#(id)","title":"","description":"","closed":false,"owner_id":null,"createdAt":1}}"#
  }

  /// The bodies of the messages of `type` the client sent on `socket`.
  func sent(_ type: String, on socket: MockSocket) -> [JSONValue] {
    socket.messages.compactMap { text in
      let m = try? JSONParser.parse(Array(text.utf8))
      return m?[0]?.stringValue == type ? m?[1] : nil
    }
  }

  /// The IDs of the mutations pushed on `socket`, in order.
  func pushedIDs(on socket: MockSocket) -> [Double] {
    sent("push", on: socket).flatMap { $0["mutations"]?.arrayValue ?? [] }
      .compactMap { $0["id"]?.numberValue }
  }

  // MARK: - Pokes

  /// zero-cache sends an empty poke (`baseCookie` == `cookie`) on every new
  /// connection so the client learns its got queries are current again
  /// (c8540af97). JS: `rep.poke` is a no-op for an unchanged cookie, but
  /// the poke callback in `zero.ts` still calls
  /// `markGotQueriesAuthoritative()`, so a query the server already has
  /// completes.
  @Test(.timeLimit(.minutes(1)))
  func emptyInitialPokeAfterReconnectMarksGotQueriesAuthoritative() throws {
    let zero = try makeZero()
    let first = try connect()
    poke(first, "p1", from: nil, to: "01", Self.gotAll + #","rowsPatch":[\#(Self.row("i1"))]"#)

    first.serverClose(code: 1006)
    let second = try connect()
    #expect(sockets.sockets.count == 2)
    #expect(zero.connection.state == .connected)

    // Subscribed after the reconnect: not trusted until the first poke.
    let view = try zero.materialize(Self.all())
    let preload = zero.preload(Self.all())
    scheduler.runDueTimers()
    #expect(view.resultType != .complete)

    // The server is caught up: the initial poke is empty.
    poke(second, "p2", from: "01", to: "01", nil)
    #expect(view.resultType == .complete)
    #expect(preload.complete.isSettled)
    #expect(view.data.json?.arrayValue?.map { $0["id"] } == ["i1"])
    view.destroy()
  }

  /// The same on a restart: the next client of the store connects to a
  /// server it is caught up with, so the first poke is empty. JS completes
  /// the query; a `run(type: .complete)` must not wait for a later change.
  @Test(.timeLimit(.minutes(1)))
  func emptyInitialPokeAfterRestartCompletesPersistedGotQueries() throws {
    var options = ZeroOptions(
      cacheURL: "http://zero.test", userID: "u", storageKey: randomID(), storage: .memory)
    options.logSink = log
    let make = {
      try Zero(
        schema: .test, mutators: [Self.create], options: options, scheduler: scheduler,
        socketFactory: sockets)
    }
    let z1 = try make()
    let first = try connect()
    poke(first, "p1", from: nil, to: "01", Self.gotAll + #","rowsPatch":[\#(Self.row("i1"))]"#)

    let z2 = try make()
    let view = try z2.materialize(Self.all())
    let run = try z2.run(Self.all().query(()), type: .complete)
    let second = try connect()
    #expect(second !== first)
    poke(second, "p1", from: "01", to: "01", nil)
    #expect(view.resultType == .complete)
    #expect(run.isSettled)
    view.destroy()
    z1.close()
    z2.close()
  }

  /// A canceled poke is dropped; the next poke on the same base applies.
  @Test func canceledPokeIsDroppedAndTheNextOneApplies() throws {
    let zero = try makeZero()
    let view = try zero.materialize(Self.all())
    let socket = try connect()
    socket.receive(text: #"["pokeStart",{"pokeID":"p1","baseCookie":null}]"#)
    socket.receive(
      text: #"["pokePart",{"pokeID":"p1","rowsPatch":[\#(Self.row("bad"))]}]"#)
    socket.receive(text: #"["pokeEnd",{"pokeID":"p1","cookie":"01","cancel":true}]"#)
    scheduler.runDueTimers()
    #expect(view.data.json == [])

    poke(socket, "p2", from: nil, to: "01", Self.gotAll + #","rowsPatch":[\#(Self.row("i1"))]"#)
    #expect(view.data.json?.arrayValue?.map { $0["id"] } == ["i1"])
    #expect(zero.connection.state == .connected)
    #expect(sockets.sockets.count == 1)
    view.destroy()
  }

  /// A poke built on a cookie the client doesn't have: JS disconnects with
  /// `UnexpectedBaseCookie` and reconnects with the cookie it has.
  @Test(.timeLimit(.minutes(1)))
  func pokeForAnOldBaseCookieReconnects() throws {
    let zero = try makeZero()
    let first = try connect()
    poke(first, "p1", from: nil, to: "02", Self.gotAll + #","rowsPatch":[\#(Self.row("i1"))]"#)
    poke(first, "p2", from: "01", to: "03", #""rowsPatch":[\#(Self.row("i2"))]"#)
    #expect(first.closed)
    // Not a dropped socket, so the run loop backs off first.
    scheduler.advance(by: 5_000)
    let second = try connect()
    #expect(second.url.contains("baseCookie=02"))
    #expect(zero.connection.state == .connected)
  }

  // MARK: - Pushes

  /// Mutations made while connecting are pushed once each, in order, when
  /// the connection is up; a reconnect pushes the unconfirmed ones again
  /// (JS clears `#lastMutationIDsSent` on connect).
  @Test(.timeLimit(.minutes(1)))
  func mutationsWhileDisconnectedArePushedOnceInOrder() throws {
    let zero = try makeZero()
    scheduler.runDueTimers()
    for id in ["a", "b", "c"] { zero.mutate(Self.create(id)) }
    scheduler.runDueTimers()
    let first = try connect()
    #expect(pushedIDs(on: first) == [1, 2, 3])

    zero.mutate(Self.create("d"))
    scheduler.runDueTimers()
    #expect(pushedIDs(on: first) == [1, 2, 3, 4])

    // The server confirms 1 and 2, then the socket drops.
    poke(first, "p1", from: nil, to: "01", #""lastMutationIDChanges":{"\#(zero.clientID)":2}"#)
    first.serverClose(code: 1006)
    let second = try connect()
    #expect(pushedIDs(on: second) == [3, 4])
  }

  /// A mutation made between `pokeStart` and `pokeEnd` is pushed once and
  /// stays pending over the poke that confirms the earlier one.
  @Test func mutationDuringAPokeIsPushedOnceAndRebased() throws {
    let zero = try makeZero()
    let view = try zero.materialize(Self.all())
    let socket = try connect()
    zero.mutate(Self.create("a"))
    scheduler.runDueTimers()

    socket.receive(text: #"["pokeStart",{"pokeID":"p1","baseCookie":null}]"#)
    socket.receive(
      text:
        #"["pokePart",{"pokeID":"p1","lastMutationIDChanges":{"\#(zero.clientID)":1},"rowsPatch":[\#(Self.row("a"))]}]"#
    )
    zero.mutate(Self.create("b"))
    socket.receive(text: #"["pokeEnd",{"pokeID":"p1","cookie":"01"}]"#)
    scheduler.runDueTimers()

    #expect(pushedIDs(on: socket) == [1, 2])
    #expect(view.data.json?.arrayValue?.map { $0["id"] } == ["a", "b"])
    #expect(zero.engine.store.pending.map(\.id) == [2])
    view.destroy()
  }

  /// A push response for a mutation the client doesn't track is ignored
  /// (JS: "Mutation already resolved or rejected").
  @Test func pushResponseForAnUnknownMutationIsIgnored() async throws {
    let zero = try makeZero()
    let socket = try connect()
    let result = zero.mutate(Self.create("a"))
    scheduler.runDueTimers()
    socket.receive(
      text:
        #"["pushResponse",{"mutations":[{"id":{"clientID":"\#(zero.clientID)","id":99},"result":{}}]}]"#
    )
    scheduler.runDueTimers()
    #expect(zero.connection.state == .connected)
    #expect(!result.server.isSettled)
    socket.receive(
      text:
        #"["pushResponse",{"mutations":[{"id":{"clientID":"\#(zero.clientID)","id":1},"result":{"data":7}}]}]"#
    )
    #expect(try await result.server.value == .success(data: 7))
  }

  /// A stored mutation result with a field a newer server added still
  /// settles the mutation, and an error stays an error (c948f562d).
  @Test func storedMutationResultWithUnknownFieldsSettles() async throws {
    let zero = try makeZero()
    let socket = try connect()
    let ok = zero.mutate(Self.create("a"))
    let failed = zero.mutate(Self.create("b"))
    scheduler.runDueTimers()
    let cid = zero.clientID
    poke(
      socket, "p1", from: nil, to: "01",
      #""lastMutationIDChanges":{"\#(cid)":2},"mutationsPatch":[{"op":"put","mutation":{"id":{"clientID":"\#(cid)","id":1},"result":{"data":1,"extra":true}}},{"op":"put","mutation":{"id":{"clientID":"\#(cid)","id":2},"result":{"error":"app","message":"no","extra":1}}}]"#
    )
    #expect(try await ok.server.value == .success(data: 1))
    #expect(try await failed.server.value == .appError(message: "no", details: nil))
  }

  // MARK: - Mutators

  /// A mutator that throws after writing leaves nothing behind, and the
  /// next mutation takes its mutation ID (Replicache doesn't commit it).
  @Test func throwingMutatorRollsBackItsWrites() async throws {
    let zero = try makeZero()
    let view = try zero.materialize(Self.all())
    let socket = try connect()
    let failed = zero.mutate(Self.createThenThrow("x"))
    scheduler.runDueTimers()
    #expect(try await failed.client.value == .appError(message: "midway", details: nil))
    #expect(view.data.json == [])
    #expect(try await zero.run(zql.issue).isEmpty)
    #expect(zero.engine.store.pending.isEmpty)

    zero.mutate(Self.create("y"))
    scheduler.runDueTimers()
    #expect(pushedIDs(on: socket) == [1])
    #expect(view.data.json?.arrayValue?.map { $0["id"] } == ["y"])
    view.destroy()
  }

  /// A mutate from inside a view listener: both mutations apply, the view
  /// ends with both rows, and each is pushed once, in order.
  @Test func mutateFromAViewListener() throws {
    let zero = try makeZero()
    let socket = try connect()
    let view = try zero.materialize(zql.issue.orderBy(\.id, .asc))
    var reentered = false
    var seen: [[JSONValue?]] = []
    _ = view.addListener { data, _, _ in
      seen.append(data.json?.arrayValue?.map { $0["id"] } ?? [])
      if !reentered, data.json?.arrayValue?.count == 1 {
        reentered = true
        zero.mutate(Self.create("b"))
      }
    }
    zero.mutate(Self.create("a"))
    scheduler.runDueTimers()
    #expect(view.data.json?.arrayValue?.map { $0["id"] } == ["a", "b"])
    #expect(seen.last == ["a", "b"])
    #expect(pushedIDs(on: socket) == [1, 2])
    #expect(zero.engine.store.pending.map(\.id) == [1, 2])
    view.destroy()
  }

  /// A materialize from inside a view listener sees the committed data.
  @Test func materializeFromAViewListener() throws {
    let zero = try makeZero()
    _ = try connect()
    let view = try zero.materialize(zql.issue.orderBy(\.id, .asc))
    var inner: MaterializedView?
    _ = view.addListener { data, _, _ in
      if inner == nil, data.json?.arrayValue?.count == 1 {
        inner = try? zero.materialize(zql.issue.orderBy(\.id, .asc))
      }
    }
    zero.mutate(Self.create("a"))
    scheduler.runDueTimers()
    #expect(inner?.data.json?.arrayValue?.map { $0["id"] } == ["a"])
    zero.mutate(Self.create("b"))
    scheduler.runDueTimers()
    #expect(inner?.data.json?.arrayValue?.map { $0["id"] } == ["a", "b"])
    inner?.destroy()
    view.destroy()
  }

  // MARK: - Views and preloads

  /// Two preloads of one query: cleaning up one of them twice keeps the
  /// other's query desired.
  @Test func preloadCleanupTwiceKeepsTheOtherPreload() throws {
    let zero = try makeZero()
    let socket = try connect()
    let a = zero.preload(Self.all())
    let b = zero.preload(Self.all())
    scheduler.advance(by: 100)
    a.cleanup()
    a.cleanup()
    scheduler.advance(by: 100)
    let ops = sent("changeDesiredQueries", on: socket).flatMap {
      $0["desiredQueriesPatch"]?.arrayValue ?? []
    }
    #expect(!ops.contains { $0["op"] == "del" })
    b.cleanup()
    scheduler.advance(by: 100)
    let after = sent("changeDesiredQueries", on: socket).flatMap {
      $0["desiredQueriesPatch"]?.arrayValue ?? []
    }
    #expect(after.filter { $0["op"] == "del" }.count == 1)
  }

  /// Two views of one query: destroying one twice keeps the other's query.
  @Test func viewDestroyedTwiceKeepsTheOtherView() throws {
    let zero = try makeZero()
    let socket = try connect()
    let a = try zero.materialize(Self.all())
    let b = try zero.materialize(Self.all())
    scheduler.advance(by: 100)
    a.destroy()
    a.destroy()
    scheduler.advance(by: 100)
    let ops = sent("changeDesiredQueries", on: socket).flatMap {
      $0["desiredQueriesPatch"]?.arrayValue ?? []
    }
    #expect(!ops.contains { $0["op"] == "del" })
    poke(socket, "p1", from: nil, to: "01", Self.gotAll + #","rowsPatch":[\#(Self.row("i1"))]"#)
    #expect(b.data.json?.arrayValue?.map { $0["id"] } == ["i1"])
    #expect(b.resultType == .complete)
    b.destroy()
  }

  /// A live query dropped right after it is made, before its query is sent:
  /// the server is never left wanting it.
  @Test func liveQueryReleasedWhileItsQueryIsBeingAdded() throws {
    let zero = try makeZero()
    let socket = try connect()
    var live: LiveQuery<[Issue]>? = try zero.liveQuery(Self.all())
    #expect(live != nil)
    live = nil
    scheduler.advance(by: 100)
    let ops = sent("changeDesiredQueries", on: socket).flatMap {
      $0["desiredQueriesPatch"]?.arrayValue ?? []
    }
    #expect(ops.last?["op"] == "del")
  }

  // MARK: - Connecting

  /// zero-cache is behind the replica: JS drops the local database and the
  /// replacement client starts from nothing (a3af877dd).
  @Test(.timeLimit(.minutes(1)))
  func invalidBaseCookieReplacesTheClientWithAnEmptyReplica() throws {
    let zero = try makeZero()
    let view = try zero.materialize(Self.all())
    let first = try connect()
    poke(first, "p1", from: nil, to: "05", Self.gotAll + #","rowsPatch":[\#(Self.row("i1"))]"#)
    #expect(view.data.json?.arrayValue?.count == 1)
    let oldClientID = zero.clientID

    first.receive(
      text:
        #"["error",{"kind":"InvalidConnectionRequestBaseCookie","message":"behind","origin":"zeroCache"}]"#
    )
    scheduler.runDueTimers()
    #expect(zero.clientID != oldClientID)
    let second = try connect()
    #expect(sockets.sockets.count == 2)
    #expect(second.url.contains("baseCookie=&"))
    #expect(view.data.json == [])
    view.destroy()
  }

  /// A got query persisted by an earlier client is only `cached` for the
  /// next one, and drops back to `unknown` when the server's first poke
  /// says it is gone (c8540af97).
  @Test(.timeLimit(.minutes(1)))
  func staleGotFromAnEarlierClientIsNotComplete() throws {
    var options = ZeroOptions(
      cacheURL: "http://zero.test", userID: "u", storageKey: randomID(), storage: .memory)
    options.logSink = log
    let make = {
      try Zero(
        schema: .test, mutators: [Self.create, Self.createThenThrow], options: options,
        scheduler: scheduler, socketFactory: sockets)
    }
    let z1 = try make()
    let first = try connect()
    poke(first, "p1", from: nil, to: "01", Self.gotAll + #","rowsPatch":[\#(Self.row("i1"))]"#)

    let z2 = try make()
    let view = try z2.materialize(Self.all())
    #expect(view.resultType == .cached)
    #expect(view.data.json?.arrayValue?.count == 1)
    let second = try connect()
    #expect(second !== first)
    poke(
      second, "p1", from: "01", to: "02",
      #""gotQueriesPatch":[{"op":"del","hash":"\#(Self.hash)"}],"rowsPatch":[{"op":"del","tableName":"issues","id":{"id":"i1"}}]"#
    )
    #expect(view.resultType == .unknown)
    #expect(view.data.json == [])
    view.destroy()
    z1.close()
    z2.close()
  }

  /// React Native reports a dropped connection as 1005: logged at info
  /// like 1006, not as an error (eeface523).
  @Test func closeCode1005IsNotLoggedAsAnError() throws {
    let zero = try makeZero()
    let first = try connect()
    first.serverClose(code: 1005)
    scheduler.runDueTimers()
    #expect(!log.entries.contains { $0.level == .error && $0.message.contains("socket close") })
    #expect(sockets.sockets.count == 2)
    _ = zero
  }

  // MARK: - Auth and close

  /// `connect(auth:)` while connecting: no `updateAuth` on the attempt's
  /// socket, and the run loop keeps the attempt.
  @Test(.timeLimit(.minutes(1)))
  func connectWithAuthWhileConnecting() throws {
    let zero = try makeZero()
    scheduler.runDueTimers()
    let socket = try #require(sockets.sockets.last)
    zero.connection.connect(auth: "t2")
    scheduler.runDueTimers()
    #expect(sent("updateAuth", on: socket).isEmpty)
    socket.receive(text: #"["connected",{"wsid":"w"}]"#)
    scheduler.runDueTimers()
    #expect(zero.connection.state == .connected)
    #expect(sockets.sockets.count == 1)
    zero.connection.connect(auth: "t3")
    #expect(sent("updateAuth", on: socket) == [["auth": "t3"]])
  }

  /// `close()` while connecting: the socket closes with 1000, pending
  /// mutations and queries fail, and nothing reconnects.
  @Test(.timeLimit(.minutes(1)))
  func closeWhileConnectingWithPendingQueries() async throws {
    let zero = try makeZero()
    let view = try zero.materialize(Self.all())
    scheduler.runDueTimers()
    let socket = try #require(sockets.sockets.last)
    let result = zero.mutate(Self.create("a"))
    let run = try zero.run(Self.all().query(()), type: .complete)
    zero.close()
    scheduler.advance(by: 60_000)
    #expect(socket.closed)
    #expect(socket.closeCode == 1000)
    #expect(sockets.sockets.count == 1)
    #expect(zero.connection.state.status == .closed)
    #expect(view.resultType == .error)
    if case .zeroError = try await result.server.value {
    } else {
      Testing.Issue.record("not rejected")
    }
    await #expect(throws: (any Error).self) { try await run.value }
    view.destroy()
  }

  /// Swift only (JS rejects only the calls pending at close, so new ones
  /// never settle): on a closed `Zero`, a new run waiting for the server
  /// and a new preload fail at once with the close error, while `.unknown`
  /// still reads the local rows.
  @Test(.timeLimit(.minutes(1)))
  func newCallsOnAClosedZeroFailAtOnce() async throws {
    let zero = try makeZero()
    zero.mutate(Self.create("a"))
    zero.close()
    let message = "Zero was explicitly closed by calling zero.close()"
    func expectClosed(_ body: () async throws -> Void) async {
      do {
        try await body()
        Testing.Issue.record("not rejected")
      } catch let error as QueryError {
        #expect(error.description == message)
      } catch {
        Testing.Issue.record("unexpected \(error)")
      }
    }
    for type in [RunType.complete, .cached] {
      #expect(try zero.run(Self.all().query(()), type: type).isSettled)
      await expectClosed { _ = try await zero.run(Self.all(), type: type) }
    }
    let preload = zero.preload(Self.all())
    #expect(preload.cached.isSettled && preload.complete.isSettled)
    await expectClosed { try await preload.cached.value }
    await expectClosed { try await preload.complete.value }
    preload.cleanup()
    #expect(try await zero.run(Self.all()).map(\.id) == ["a"])
  }
}
