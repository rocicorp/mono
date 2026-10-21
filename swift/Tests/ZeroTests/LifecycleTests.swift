import Testing
import ZeroTesting

@testable import Zero

/// What the native client adds to the JS client's recovery: replacing the
/// client in place, reconnecting when the network is back, and waiting for
/// pending mutations before the app is suspended.
@Suite @MainActor struct LifecycleTests {
  static let all = defineQuery("issues.all") { () in zql.issue.orderBy(\.id, .asc) }
  static let closed = defineQuery("issues.closed") { () in zql.issue.where(\.closed, true) }
  static let create = defineMutator("issue.create") { (tx, id: String) in
    try tx.mutate.issue.insert([
      "id": .string(id), "title": "", "description": "", "closed": false, "ownerId": .null,
      "createdAt": 1,
    ])
  }

  let scheduler = ManualScheduler(now: 1_700_000_000_000)
  let sockets = MockSocketFactory()
  let log = RecordingLogSink()

  func makeZero() throws -> Zero<Void> {
    var options = ZeroOptions(
      cacheURL: "http://zero.test", userID: "u", storageKey: randomID(), storage: .memory)
    options.logSink = log
    return try Zero(
      schema: .test, mutators: [Self.create], options: options, scheduler: scheduler,
      socketFactory: sockets)
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
    _ socket: MockSocket, _ id: String, from base: String?, to cookie: String, _ part: String
  ) {
    let base = base.map { "\"\($0)\"" } ?? "null"
    socket.receive(text: #"["pokeStart",{"pokeID":"\#(id)","baseCookie":\#(base)}]"#)
    socket.receive(text: #"["pokePart",{"pokeID":"\#(id)",\#(part)}]"#)
    socket.receive(text: #"["pokeEnd",{"pokeID":"\#(id)","cookie":"\#(cookie)"}]"#)
    scheduler.runDueTimers()
  }

  static let hash = QueryHash.hashOfNameAndArgs("issues.all", [])
  static func row(_ id: String) -> String {
    #"{"op":"put","tableName":"issues","value":{"id":"\#(id)","title":"","description":"","closed":false,"owner_id":null,"createdAt":1}}"#
  }

  @Test func replacesAClientTheServerLostAndKeepsItsViews() throws {
    let zero = try makeZero()
    let view = try zero.materialize(Self.all())
    let live = try zero.liveQuery(Self.all())
    _ = zero.preload(Self.closed())
    let first = try connect()
    poke(
      first, "p1", from: nil, to: "01",
      #""gotQueriesPatch":[{"op":"put","hash":"\#(Self.hash)"}],"rowsPatch":[\#(Self.row("i1"))]"#)
    #expect(live.data.map(\.id) == ["i1"])
    let old = (zero.clientID, zero.clientGroupID)

    first.receive(
      text: #"["error",{"kind":"ClientNotFound","message":"gone","origin":"zeroCache"}]"#)
    scheduler.runDueTimers()

    // A new client in a new group; the old group is disabled, so its rows
    // are gone until the new client syncs.
    #expect(zero.clientID != old.0)
    #expect(zero.clientGroupID != old.1)
    #expect(view.data.json == [])
    #expect(live.data.isEmpty)
    let second = try connect()
    #expect(sockets.sockets.count == 2)
    let initConnection = try SecProtocols.decode(#require(second.secProtocol))[
      "initConnectionMessage"]?[1]
    #expect(
      Set(initConnection?["desiredQueriesPatch"]?.arrayValue?.compactMap { $0["name"] } ?? [])
        == ["issues.all", "issues.closed"])
    #expect(zero.connection.state == .connected)

    poke(
      second, "p1", from: nil, to: "01",
      #""gotQueriesPatch":[{"op":"put","hash":"\#(Self.hash)"}],"rowsPatch":[\#(Self.row("i2"))]"#)
    #expect(view.data.json?.arrayValue?.map { $0["id"] } == ["i2"])
    #expect(live.data.map(\.id) == ["i2"])
    #expect(view.resultType == .complete)
    #expect(log.entries.contains { $0.level == .warn && $0.message == "ClientNotFound:\n\ngone" })
  }

  @Test func reconnectsWithoutTheBackoffWhenTheNetworkIsBack() throws {
    let zero = try makeZero()
    let first = try connect()
    // A dropped connection reconnects at once; a failed attempt backs off.
    first.serverClose(code: 1006)
    scheduler.runDueTimers()
    #expect(sockets.sockets.count == 2)
    sockets.sockets[1].serverClose(code: 1006)
    scheduler.advance(by: 1000)
    #expect(sockets.sockets.count == 2)

    zero.reconnectNow()
    scheduler.runDueTimers()
    #expect(sockets.sockets.count == 3)
  }

  @Test func checksAnOpenConnectionWhenTheNetworkChanges() throws {
    let zero = try makeZero()
    let socket = try connect()
    zero.reconnectNow()
    scheduler.runDueTimers()
    #expect(socket.messages.last == #"["ping",{}]"#)
  }

  @Test func waitsForTheServerToConfirmPendingMutations() throws {
    let zero = try makeZero()
    let socket = try connect()
    zero.mutate(Self.create("i1"))
    scheduler.runDueTimers()
    var settled = false
    zero.whenPendingSettled { settled = true }
    scheduler.runDueTimers()
    #expect(!settled)

    poke(socket, "p1", from: nil, to: "01", #""lastMutationIDChanges":{"\#(zero.clientID)":1}"#)
    #expect(settled)
  }

  @Test func disconnectsInTheBackgroundAndReconnectsInTheForeground() throws {
    let zero = try makeZero()
    let first = try connect()
    zero.setVisible(false)
    scheduler.runDueTimers()
    #expect(first.closed)
    #expect(
      zero.connection.state == .disconnected(reason: "Connection closed because tab was hidden"))
    scheduler.advance(by: 60_000)
    #expect(sockets.sockets.count == 1)

    zero.setVisible(true)
    _ = try connect()
    #expect(sockets.sockets.count == 2)
    #expect(zero.connection.state == .connected)
  }
}
