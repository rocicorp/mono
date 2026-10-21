import Foundation
import Testing
import ZeroTesting

@testable import Zero

/// What the client stores on disk beyond what the client scenarios check:
/// falling back to memory, and collecting old databases.
@Suite @MainActor struct StorageTests {
  static let day: Double = 24 * 60 * 60_000
  let directory = NSTemporaryDirectory() + "zero-storage-tests-" + UUID().uuidString

  func makeZero(
    _ storageKey: String, at now: Double, storage: ZeroStorage? = nil,
    onStorageFailure: (@MainActor @Sendable (StorageFailure) -> Void)? = nil
  ) throws -> (Zero<Void>, ManualScheduler) {
    let scheduler = ManualScheduler(now: now)
    var options = ZeroOptions(
      userID: "u", storageKey: storageKey, storage: storage ?? .sqlite(directory: directory))
    options.onStorageFailure = onStorageFailure
    options.logSink = RecordingLogSink()
    let zero = try Zero(
      schema: .test, mutators: [create], options: options, scheduler: scheduler,
      socketFactory: nil)
    return (zero, scheduler)
  }

  let create = defineMutator("issue.create") { (tx, id: String) in
    try tx.mutate.issue.insert([
      "id": .string(id), "title": "", "description": "", "closed": false, "ownerId": .null,
      "createdAt": 1,
    ])
  }

  func fileName(_ storageKey: String) -> String {
    ReplicaDatabase.fileName(
      ReplicaDatabase.name(
        userID: "u", storageKey: storageKey, mutateURL: nil, queryURL: nil,
        schemaHash: ZeroSchema.test.clientSchema.hash))
  }

  func files() throws -> Set<String> {
    Set(
      try FileManager.default.contentsOfDirectory(atPath: directory).filter {
        $0.hasSuffix(".sqlite")
      })
  }

  @Test func runsOnMemoryWhenTheDatabaseCannotOpen() async throws {
    final class Failures: @unchecked Sendable { var kinds: [StorageFailure.Kind] = [] }
    let failures = Failures()
    let (zero, scheduler) = try makeZero(
      "k", at: 0, storage: .sqlite(directory: "/dev/null/zero"),
      onStorageFailure: { failures.kinds.append($0.kind) })
    zero.mutate(create("i1"))
    scheduler.runDueTimers()
    #expect(failures.kinds == [.cannotOpen])
    #expect(try await zero.run(zql.issue).map(\.id) == ["i1"])
    zero.close()
  }

  @Test func keepsMutationsAcrossLaunches() async throws {
    defer { try? FileManager.default.removeItem(atPath: directory) }
    let (first, _) = try makeZero("k", at: 0)
    first.mutate(create("i1"))
    first.close()

    let (second, _) = try makeZero("k", at: 1000)
    #expect(second.clientGroupID == first.clientGroupID)
    #expect(try await second.run(zql.issue).map(\.id) == ["i1"])
    #expect(second.engine.store.pending.map(\.clientID) == [first.clientID])
    second.close()
  }

  @Test func collectsDatabasesNoClientUsedForTwoDays() throws {
    defer { try? FileManager.default.removeItem(atPath: directory) }
    let (old, _) = try makeZero("old", at: 0)
    old.close()
    let (recent, _) = try makeZero("recent", at: 60 * 60_000)
    recent.close()

    // Two days and a minute after the old one last ran.
    let (current, scheduler) = try makeZero("current", at: 2 * Self.day + 60_000)
    scheduler.advance(by: 5 * 60_000 - 1)
    current.engine.database.flush()
    #expect(try files() == [fileName("old"), fileName("recent"), fileName("current")])

    scheduler.advance(by: 1)
    current.engine.database.flush()
    #expect(try files() == [fileName("recent"), fileName("current")])
    current.close()
  }
}
