#if canImport(UIKit) || canImport(AppKit)
  import Testing
  import ZeroTesting
  import ZeroUI

  @testable import Zero

  @Suite @MainActor struct DiffableAdapterTests {
    func makeZero() throws -> Zero<Void> {
      var options = ZeroOptions(userID: "u", storageKey: randomID(), storage: .memory)
      options.logSink = RecordingLogSink()
      return try Zero(
        schema: .notes, mutators: [Notes.put, Notes.remove], options: options,
        scheduler: ManualScheduler(now: 0), socketFactory: nil)
    }

    @Test func appliesASnapshotPerChangeKeyedByPrimaryKey() throws {
      let zero = try makeZero()
      var applied: [(snapshot: DiffableAdapter<Note, Int>.Snapshot, animating: Bool)] = []
      let adapter = DiffableAdapter<Note, Int>(
        try zero.materialize(Notes.all()), section: 0
      ) { applied.append(($0, $1)) }

      zero.mutate(Notes.put(.init(id: "b", text: "2")))
      zero.mutate(Notes.put(.init(id: "a", text: "1")))
      zero.mutate(Notes.put(.init(id: "a", text: "1!")))
      zero.mutate(Notes.remove("b"))

      let a = RowID(["a"])
      let b = RowID(["b"])
      #expect(applied.map(\.snapshot.sectionIdentifiers) == Array(repeating: [0], count: 5))
      #expect(applied.map(\.snapshot.itemIdentifiers) == [[], [b], [a, b], [a, b], [a]])
      #if canImport(UIKit)
        #expect(applied.map(\.snapshot.reconfiguredItemIdentifiers) == [[], [], [], [a], []])
      #endif
      #expect(applied.map(\.animating) == [false, true, true, true, true])
      #expect(adapter.row(for: a) == Note(id: "a", text: "1!"))
      #expect(adapter.row(for: b) == nil)
    }
  }
#endif
