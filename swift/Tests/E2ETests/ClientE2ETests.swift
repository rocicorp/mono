import Foundation
import Testing
import Zero

/// The app of the end-to-end server (`packages/zero-conformance/src/e2e/app.ts`),
/// defined in Swift as an app would.
enum E2EApp {
  static let open = defineQuery("issues.open") { () in
    testZQL.issue.where(\.closed, false).related(\.comments).related(\.owner)
      .orderBy(\.createdAt, .asc)
  }
  static let byID = defineQuery("issues.byID") { (id: String) in
    testZQL.issue.where(\.id, id).related(\.comments).one()
  }

  struct CreateIssue: Codable, Sendable {
    var id: String
    var title: String
    var ownerId: String?
  }

  static let create = defineMutator("issue.create") { (tx, args: CreateIssue) in
    try tx.mutate.insert(
      TestIssue(
        id: args.id, title: args.title, description: "", closed: false, ownerId: args.ownerId,
        createdAt: 100))
  }
  static let close = defineMutator("issue.close") { (tx, id: String) in
    try tx.mutate.issue.update(["id": .string(id), "closed": true])
  }
}

/// The whole client against zero-cache, the API server, and Postgres: sync,
/// optimistic mutations the server confirms, and a relaunch on the same
/// store.
@Suite(.enabled(if: E2E.url != nil), .timeLimit(.minutes(2)))
@MainActor
struct ClientE2ETests {
  let directory = NSTemporaryDirectory() + "zero-e2e-" + UUID().uuidString

  func makeZero(storageKey: String) throws -> Zero<Void> {
    let origin = E2E.url!.replacingOccurrences(of: "ws://", with: "http://")
    let options = ZeroOptions(
      cacheURL: origin, storageKey: storageKey, storage: .sqlite(directory: directory))
    return try Zero(
      schema: .test, mutators: [E2EApp.create, E2EApp.close], options: options,
      socketFactory: TransportSocketFactory(NIOTransport()))
  }

  /// Waits until `condition` holds.
  func eventually(
    _ what: String, timeout: Duration = .seconds(30), _ condition: () -> Bool
  ) async throws {
    let deadline = ContinuousClock.now + timeout
    while !condition() {
      guard ContinuousClock.now < deadline else {
        Issue.record("Timed out waiting for \(what)")
        throw CancellationError()
      }
      try await Task.sleep(for: .milliseconds(20))
    }
  }

  @Test func syncsMutatesAndRelaunches() async throws {
    defer { try? FileManager.default.removeItem(atPath: directory) }
    let key = UUID().uuidString
    let zero = try makeZero(storageKey: key)
    let open = try zero.liveQuery(E2EApp.open())
    try await eventually("the open issues") { open.resultType == .complete }
    #expect(Set(["i1", "i3", "i4"]).isSubset(of: open.data.map(\.id)))
    let i1 = try #require(open.data.first { $0.id == "i1" })
    #expect(i1.comments?.map(\.id) == ["c1", "c2"])
    #expect(i1.owner?.name == "Alice")
    #expect(open.data.first { $0.id == "i3" }?.title == "Emoji 😀 in titles")

    // Optimistic at once, then confirmed by the server.
    let id = "swift-\(UUID().uuidString)"
    let created = zero.mutate(E2EApp.create(.init(id: id, title: "From Swift", ownerId: "u2")))
    #expect(open.data.contains { $0.id == id })
    #expect(try await created.client.value == .success(data: nil))
    #expect(try await created.server.value == .success(data: nil))
    let confirmed = try await zero.run(E2EApp.byID(id), type: .complete)
    #expect(confirmed?.title == "From Swift")

    // Another client sees it through the server.
    let other = try makeZero(storageKey: UUID().uuidString)
    let seen = try await other.run(E2EApp.byID(id), type: .complete)
    #expect(seen?.ownerId == "u2")

    let closed = zero.mutate(E2EApp.close(id))
    #expect(try await closed.server.value == .success(data: nil))
    #expect(!open.data.contains { $0.id == id })
    let liveOther = try other.liveQuery(E2EApp.byID(id))
    try await eventually("the close to sync") { liveOther.data?.closed == true }
    other.close()

    // A relaunch reads the rows from its store before the server answers.
    zero.close()
    let relaunched = try makeZero(storageKey: key)
    defer { relaunched.close() }
    #expect(relaunched.clientGroupID == zero.clientGroupID)
    let cached = try await relaunched.run(E2EApp.open(), type: .cached)
    #expect(Set(["i1", "i3", "i4"]).isSubset(of: cached.map(\.id)))
    #expect(!cached.contains { $0.id == id })
  }

  /// Two clients of one client group (two windows) both keep their
  /// queries: each lists the other in `activeClients`, so zero-cache
  /// doesn't delete it.
  @Test func twoClientsOfAGroupBothKeepTheirQueries() async throws {
    defer { try? FileManager.default.removeItem(atPath: directory) }
    let key = UUID().uuidString
    let first = try makeZero(storageKey: key)
    defer { first.close() }
    // Without a TTL, a query of a client the server deletes goes at once.
    let open = try first.liveQuery(E2EApp.open(), ttl: .none)
    try await eventually("the first client's issues") { open.resultType == .complete }

    let second = try makeZero(storageKey: key)
    defer { second.close() }
    #expect(second.clientGroupID == first.clientGroupID)
    let byID = try second.liveQuery(E2EApp.byID("i1"))
    try await eventually("the second client's issue") { byID.resultType == .complete }

    // The first client's query still syncs a row only it asks for.
    let id = "swift-\(UUID().uuidString)"
    let created = second.mutate(E2EApp.create(.init(id: id, title: "Two windows", ownerId: nil)))
    #expect(try await created.server.value == .success(data: nil))
    try await eventually("the first client to see the new issue") {
      open.data.contains { $0.id == id }
    }
    #expect(open.resultType == .complete)
    #expect(byID.data?.id == "i1")
    let closed = second.mutate(E2EApp.close(id))
    #expect(try await closed.server.value == .success(data: nil))
  }
}
