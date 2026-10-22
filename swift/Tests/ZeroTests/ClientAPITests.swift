import Testing
import ZeroTesting

@testable import Zero

/// The typed definitions through `Zero`: argument encoding on the wire,
/// typed results, and the data a mutator returns. The engine's behavior is
/// covered by the client scenario vectors.
@Suite @MainActor struct ClientAPITests {
  struct ByOwner: Codable, Sendable { var ownerId: String }
  struct CreateIssue: Codable, Sendable {
    var id: String
    var title: String
    var ownerId: String?
  }
  struct Auth: Sendable { var userID: String }

  static let byOwner = defineQuery("issues.byOwner") { (args: ByOwner, _: Auth) in
    zql.issue.where(\.ownerId, args.ownerId).orderBy(\.id, .asc)
  }
  static let mine = defineQuery("issues.mine") { (_: NoArgs, auth: Auth) in
    zql.issue.where(\.ownerId, auth.userID).orderBy(\.id, .asc)
  }
  static let create = defineMutator("issue.create") { (tx, args: CreateIssue, auth: Auth) in
    try tx.mutate.insert(
      Issue(
        id: args.id, title: args.title, description: "", closed: false,
        ownerId: args.ownerId ?? auth.userID, createdAt: 1))
  }
  static let closeMine = defineMutator("issue.closeMine") { (tx, _: NoArgs, auth: Auth) in
    for issue in try tx.run(zql.issue.where(\.ownerId, auth.userID)) {
      try tx.mutate.issue.update(["id": .string(issue.id), "closed": true])
    }
  }
  struct Created: Encodable { var created: String }
  static let createReturning = defineMutator("issue.createReturning") {
    (tx, args: CreateIssue, _: Auth) -> Created in
    try tx.mutate.issue.insert([
      "id": .string(args.id), "title": .string(args.title), "description": "",
      "closed": false, "ownerId": .null, "createdAt": 1,
    ])
    return Created(created: args.id)
  }

  /// A store of the test's own, in memory.
  static var options: ZeroOptions {
    var options = ZeroOptions(
      cacheURL: "http://zero.test", userID: "u2", storageKey: randomID(), storage: .memory)
    options.logSink = RecordingLogSink()
    return options
  }

  let scheduler = ManualScheduler(now: 1_700_000_000_000)
  let sockets = MockSocketFactory()

  func makeZero() throws -> Zero<Auth> {
    try Zero(
      schema: .test, context: Auth(userID: "u2"),
      mutators: [Self.create, Self.createReturning, Self.closeMine],
      options: Self.options, scheduler: scheduler,
      socketFactory: sockets)
  }

  func sent(_ type: String) -> [JSONValue] {
    sockets.sockets.flatMap(\.messages).compactMap { text in
      let m = try? JSONParser.parse(Array(text.utf8))
      return m?[0]?.stringValue == type ? m?[1] : nil
    }
  }

  @Test func sendsTheEncodedArgumentsOfAQuery() throws {
    let zero = try makeZero()
    let request = Self.byOwner(ByOwner(ownerId: "u1"))
    #expect(request.args == [["ownerId": "u1"]])
    let views = try [zero.materialize(request), zero.materialize(Self.mine())]
    defer { views.forEach { $0.destroy() } }
    scheduler.runDueTimers()

    let socket = try #require(sockets.sockets.first)
    let protocols = try SecProtocols.decode(#require(socket.secProtocol))
    let patch = protocols["initConnectionMessage"]?[1]?["desiredQueriesPatch"]?.arrayValue ?? []
    #expect(
      patch.map { [$0["name"], $0["args"], $0["hash"]] } == [
        [
          "issues.byOwner", [["ownerId": "u1"]],
          .string(QueryHash.hashOfNameAndArgs("issues.byOwner", [["ownerId": "u1"]])),
        ],
        ["issues.mine", [], .string(QueryHash.hashOfNameAndArgs("issues.mine", []))],
      ])
  }

  @Test func runsMutatorsWithTypedArgumentsAndContext() async throws {
    let zero = try makeZero()
    scheduler.runDueTimers()
    let socket = try #require(sockets.sockets.first)
    socket.receive(text: #"["connected",{"wsid":"w1"}]"#)
    scheduler.runDueTimers()

    let a = zero.mutate(Self.create(CreateIssue(id: "i1", title: "mine")))
    zero.mutate(Self.create(CreateIssue(id: "i2", title: "theirs", ownerId: "u1")))
    zero.mutate(Self.closeMine())
    scheduler.runDueTimers()

    #expect(try await a.client.value == .success(data: nil))
    // The context fills the owner of the first; the second names its own.
    #expect(try await zero.run(Self.mine()).map { [$0.id: $0.closed] } == [["i1": true]])
    #expect(try await zero.run(zql.issue.where(\.closed, false)).map(\.id) == ["i2"])
    #expect(try await zero.run(Self.byOwner(ByOwner(ownerId: "u1"))).map(\.id) == ["i2"])
    #expect(
      sent("push").flatMap { $0["mutations"]?.arrayValue ?? [] }.map { [$0["name"], $0["args"]] }
        == [
          ["issue.create", [["id": "i1", "title": "mine"]]],
          ["issue.create", [["id": "i2", "title": "theirs", "ownerId": "u1"]]],
          ["issue.closeMine", [nil]],
        ])
  }

  @Test func returnsTheEncodedResultOfAMutator() async throws {
    let zero = try makeZero()
    let result = zero.mutate(Self.createReturning(CreateIssue(id: "i3", title: "t")))
    #expect(try await result.client.value == .success(data: ["created": "i3"]))
    #expect(try await zero.run(zql.issue).map(\.title) == ["t"])
  }

  @Test func anUnregisteredMutatorFailsWithoutWritingOrPushing() async throws {
    let zero = try makeZero()
    scheduler.runDueTimers()
    let socket = try #require(sockets.sockets.first)
    socket.receive(text: #"["connected",{"wsid":"w1"}]"#)
    scheduler.runDueTimers()
    let unregistered = defineMutator("issue.unregistered") { (tx, args: CreateIssue, _: Auth) in
      try tx.mutate.issue.insert([
        "id": .string(args.id), "title": .string(args.title), "description": "",
        "closed": false, "ownerId": .null, "createdAt": 1,
      ])
    }

    let result = zero.mutate(unregistered(CreateIssue(id: "i4", title: "t")))
    scheduler.runDueTimers()

    let error = MutationOutcome.zeroError(
      message: "Mutator \"issue.unregistered\" is not registered. "
        + "Mutators must be registered with the Zero constructor before use.")
    #expect(try await result.client.value == error)
    #expect(try await result.server.value == error)
    #expect(try await zero.run(zql.issue).isEmpty)
    #expect(sent("push").isEmpty)
  }

  @Test func dropsTheQueryOfAViewThatIsNoLongerReferenced() throws {
    var zero: Zero<Auth>? = try makeZero()
    var dropped: MaterializedView? = try zero!.materialize(Self.mine())
    let outliving = try zero!.materialize(zql.issue.related(\.comments).related(\.owner))
    scheduler.runDueTimers()
    let socket = try #require(sockets.sockets.first)
    socket.receive(text: #"["connected",{"wsid":"w1"}]"#)
    scheduler.runDueTimers()
    #expect(dropped != nil)

    dropped = nil
    scheduler.advance(by: 100)
    let patches = sent("changeDesiredQueries").flatMap {
      $0["desiredQueriesPatch"]?.arrayValue ?? []
    }
    #expect(
      patches.map { [$0["op"], $0["hash"]] }.last
        == ["del", .string(QueryHash.hashOfNameAndArgs("issues.mine", []))])

    // A view may outlive its client.
    zero = nil
    scheduler.advance(by: 100_000)
    #expect(outliving.data.json == [])
    outliving.destroy()
  }

  @Test(.timeLimit(.minutes(1))) func aCancelledRunThrowsAndReleasesItsQuery() async throws {
    let zero = try makeZero()
    scheduler.runDueTimers()
    let socket = try #require(sockets.sockets.first)
    socket.receive(text: #"["connected",{"wsid":"w1"}]"#)
    scheduler.runDueTimers()

    // The server never confirms the rows.
    let run = Task { try await zero.run(Self.mine(), type: .complete) }
    let mine: JSONValue? = .string(QueryHash.hashOfNameAndArgs("issues.mine", []))
    func patches() -> [JSONValue] {
      sent("changeDesiredQueries").flatMap { $0["desiredQueriesPatch"]?.arrayValue ?? [] }
    }
    for _ in 0..<100 where patches().isEmpty {
      await Task.yield()
      scheduler.advance(by: 10)
    }
    #expect(patches().map { [$0["op"], $0["hash"]] } == [["put", mine]])

    run.cancel()
    await #expect(throws: CancellationError.self) { try await run.value }
    scheduler.advance(by: 100)
    #expect(patches().map { [$0["op"], $0["hash"]] }.last == ["del", mine])
  }

  @Test func rejectsAMutatorNameRegisteredTwice() {
    #expect(throws: (any Error).self) {
      try Zero(
        schema: .test, context: Auth(userID: "u2"), mutators: [Self.create, Self.create],
        options: Self.options, scheduler: scheduler,
        socketFactory: sockets)
    }
  }
}
