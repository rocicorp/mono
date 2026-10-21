import Observation
import Testing
import ZeroTesting

@testable import Zero

/// What views add to the JS views: row diffs between snapshots, and live
/// queries that Observation tracks.
@Suite @MainActor struct ViewTests {
  struct Edit: Codable, Sendable {
    var id: String
    var title: String
  }

  static let create = defineMutator("issue.create") { (tx, args: Edit) in
    try tx.mutate.issue.insert([
      "id": .string(args.id), "title": .string(args.title), "description": "", "closed": false,
      "ownerId": .null, "createdAt": 1,
    ])
  }
  static let retitle = defineMutator("issue.retitle") { (tx, args: Edit) in
    try tx.mutate.issue.update(["id": .string(args.id), "title": .string(args.title)])
  }
  static let remove = defineMutator("issue.remove") { (tx, id: String) in
    try tx.mutate.issue.delete(["id": .string(id)])
  }
  static let comment = defineMutator("comment.create") { (tx, args: Edit) in
    try tx.mutate.comment.insert([
      "id": .string("c-\(args.id)"), "authorId": "u", "issueId": .string(args.id),
      "text": .string(args.title), "createdAt": 1,
    ])
  }

  func makeZero() throws -> Zero<Void> {
    var options = ZeroOptions(userID: "u", storageKey: randomID(), storage: .memory)
    options.logSink = RecordingLogSink()
    return try Zero(
      schema: .test, mutators: [Self.create, Self.retitle, Self.remove, Self.comment],
      options: options,
      scheduler: ManualScheduler(now: 0), socketFactory: nil)
  }

  @Test func diffsTheRowsOfEachSnapshotAgainstThePreviousOne() throws {
    let zero = try makeZero()
    let view = try zero.materialize(zql.issue.orderBy(\.title, .asc))
    var snapshots: [ViewSnapshot] = []
    _ = view.addSnapshotListener { snapshots.append($0) }

    zero.mutate(Self.create(Edit(id: "b", title: "2")))
    zero.mutate(Self.create(Edit(id: "a", title: "1")))
    zero.mutate(Self.retitle(Edit(id: "b", title: "2!")))
    // Moves `a` after `b`.
    zero.mutate(Self.retitle(Edit(id: "a", title: "3")))
    zero.mutate(Self.remove("b"))

    let a = RowID(["a"])
    let b = RowID(["b"])
    #expect(snapshots.map(\.ids) == [[], [b], [a, b], [a, b], [b, a], [a]])
    #expect(
      snapshots.map { Array($0.difference) } == [
        [], [.insert(offset: 0, element: b, associatedWith: nil)],
        [.insert(offset: 0, element: a, associatedWith: nil)], [],
        [
          .remove(offset: 0, element: a, associatedWith: nil),
          .insert(offset: 1, element: a, associatedWith: nil),
        ],
        [.remove(offset: 0, element: b, associatedWith: nil)],
      ])
    #expect(snapshots.map(\.updated) == [[], [], [], [b], [a], []])
    #expect(
      snapshots.last?.data.json == [.object(Self.rest.merging(["id": "a", "title": "3"]) { $1 })])
  }

  static let rest: [String: JSONValue] = [
    "description": "", "closed": false, "ownerId": nil, "createdAt": 1,
  ]

  @Test func streamsSnapshotsUntilTheConsumerStops() async throws {
    let zero = try makeZero()
    let view = try zero.materialize(zql.issue.orderBy(\.id, .asc))
    var iterator = view.snapshots.makeAsyncIterator()
    #expect(await iterator.next()?.ids == [])
    zero.mutate(Self.create(Edit(id: "a", title: "")))
    zero.mutate(Self.create(Edit(id: "b", title: "")))
    #expect(await iterator.next()?.ids == [RowID(["a"])])
    #expect(await iterator.next()?.ids == [RowID(["a"]), RowID(["b"])])
  }

  @Test func liveQueriesAreTrackedByObservation() throws {
    let zero = try makeZero()
    let live = try zero.liveQuery(zql.issue.orderBy(\.id, .asc))
    #expect(live.data.isEmpty)

    final class Flag: @unchecked Sendable { var raised = false }
    let changed = Flag()
    withObservationTracking {
      _ = live.data
    } onChange: {
      changed.raised = true
    }
    zero.mutate(Self.create(Edit(id: "a", title: "A")))
    #expect(changed.raised)
    #expect(live.data.map(\.title) == ["A"])
    #expect(live.resultType == .unknown)
  }

  /// A live query reads again only the rows that changed, and must read
  /// what reading every row would.
  @Test func liveQueriesKeepUpWithEachKindOfChange() throws {
    let zero = try makeZero()
    let live = try zero.liveQuery(zql.issue.related(\.comments).orderBy(\.title, .asc))
    let steps = [
      Self.create(Edit(id: "a", title: "1")), Self.create(Edit(id: "b", title: "2")),
      Self.create(Edit(id: "c", title: "3")), Self.create(Edit(id: "d", title: "4")),
      // In place, then the first row, a row between two, and the last row
      // to the front.
      Self.retitle(Edit(id: "b", title: "2!")), Self.remove("a"),
      Self.create(Edit(id: "e", title: "2.5")), Self.retitle(Edit(id: "d", title: "0")),
      // A related row.
      Self.comment(Edit(id: "e", title: "hi")),
    ]
    for step in steps {
      zero.mutate(step)
      #expect(live.data == (try [Issue](view: live.view.data)))
    }
    #expect(live.data.map(\.id) == ["d", "b", "e", "c"])
    #expect(live.data.map(\.title) == ["0", "2!", "2.5", "3"])
    #expect(live.data[2].comments?.map(\.text) == ["hi"])
  }
}
