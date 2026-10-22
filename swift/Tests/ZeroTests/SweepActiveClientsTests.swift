import Foundation
import Testing
import ZeroTesting

@testable import Zero

/// Two clients of one client group in one process, checked against the JS
/// client (`packages/zero-client` active clients).
@Suite(.serialized) @MainActor struct SweepActiveClientsTests {
  static let create = defineMutator("issue.create") { (tx, id: String) in
    try tx.mutate.issue.insert([
      "id": .string(id), "title": "", "description": "", "closed": false, "ownerId": .null,
      "createdAt": 1,
    ])
  }

  func makeZero(
    storageKey: String, sockets: MockSocketFactory, scheduler: ManualScheduler
  ) throws -> Zero<Void> {
    var options = ZeroOptions(
      cacheURL: "http://zero.test", userID: "u", storageKey: storageKey, storage: .memory)
    options.logSink = RecordingLogSink()
    return try Zero(
      schema: .test, mutators: [Self.create], options: options, scheduler: scheduler,
      socketFactory: sockets)
  }

  // MARK: - Two clients in one client group

  /// Opens the newest socket and acknowledges it.
  func connect(_ sockets: MockSocketFactory, _ scheduler: ManualScheduler) throws -> MockSocket {
    scheduler.runDueTimers()
    let socket = try #require(sockets.sockets.last)
    socket.receive(text: #"["connected",{"wsid":"w"}]"#)
    scheduler.runDueTimers()
    return socket
  }

  /// JS lists every client of the group running in the process
  /// (`ActiveClientsManager`; without `navigator.locks` its fallback lock
  /// manager is process-wide). zero-cache deletes the clients of the group
  /// that are not in `activeClients` (`view-syncer.ts`), so listing only
  /// itself makes the server delete the other client's queries while it
  /// runs.
  @Test func initConnectionListsTheOtherActiveClientOfTheGroup() throws {
    let scheduler = ManualScheduler(now: 1_700_000_000_000)
    let sockets = MockSocketFactory()
    let key = randomID()
    let first = try makeZero(storageKey: key, sockets: sockets, scheduler: scheduler)
    _ = try connect(sockets, scheduler)
    let second = try makeZero(storageKey: key, sockets: sockets, scheduler: scheduler)
    #expect(second.clientGroupID == first.clientGroupID)
    let socket = try connect(sockets, scheduler)
    let initConnection = try SecProtocols.decode(#require(socket.secProtocol))[
      "initConnectionMessage"]?[1]
    let active = Set(initConnection?["activeClients"]?.arrayValue?.compactMap(\.stringValue) ?? [])
    #expect(active == [first.clientID, second.clientID])
    first.close()
    second.close()
  }

  /// JS: "We should send a deleteClient when a Zero instance is closed"
  /// (`zero.test.ts`): the other client of the group sends `deleteClients`
  /// for the closed one, once.
  @Test func closingAClientMakesTheOtherOfTheGroupDeleteIt() throws {
    let scheduler = ManualScheduler(now: 1_700_000_000_000)
    let sockets = MockSocketFactory()
    let key = randomID()
    let first = try makeZero(storageKey: key, sockets: sockets, scheduler: scheduler)
    _ = try connect(sockets, scheduler)
    let second = try makeZero(storageKey: key, sockets: sockets, scheduler: scheduler)
    let socket = try connect(sockets, scheduler)
    #expect(second.clientGroupID == first.clientGroupID)

    first.close()
    scheduler.runDueTimers()
    let deletes = socket.messages.filter { $0.hasPrefix(#"["deleteClients""#) }
    #expect(deletes == [#"["deleteClients",{"clientIDs":["\#(first.clientID)"]}]"#])
    second.close()
  }
}
