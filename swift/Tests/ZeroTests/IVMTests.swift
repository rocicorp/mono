import Testing

@testable import Zero

/// A smoke test of the query pipeline for builds without the conformance
/// vectors; `IVMConformanceTests` checks the engine against JS.
@Suite struct IVMTests {
  static let schema = ZeroSchema(
    tables: [
      TableSchema(
        name: "issue",
        columns: [("id", ColumnSchema(.string)), ("title", ColumnSchema(.string))],
        primaryKey: ["id"]),
      TableSchema(
        name: "comment",
        columns: [
          ("id", ColumnSchema(.string)), ("issueID", ColumnSchema(.string)),
          ("body", ColumnSchema(.string)),
        ],
        primaryKey: ["id"]),
    ],
    relationships: [
      "issue": [
        "comments": [
          RelationshipHop(
            sourceField: ["id"], destField: ["issueID"], destSchema: "comment", cardinality: .many)
        ]
      ]
    ])

  final class Delegate: BuilderDelegate {
    let sources: [String: MemorySource]
    init(_ sources: [String: MemorySource]) { self.sources = sources }
    func getSource(_ tableName: String) -> (any Source)? { sources[tableName] }
  }

  @Test func materializesAndUpdates() throws {
    let issues = MemorySource(Self.schema.tables["issue"]!)
    let comments = MemorySource(Self.schema.tables["comment"]!)
    for (id, title) in [("i1", "b"), ("i2", "a"), ("i3", "c")] {
      issues.push(.add(["id": .string(id), "title": .string(title)]))
    }
    comments.push(.add(["id": "c1", "issueID": "i2", "body": "x"]))

    let query = AnyQuery(Self.schema, "issue")
      .whereExists("comments")
      .related("comments") { $0.orderBy("id", .desc) }
      .orderBy("title", .asc)
      .limit(2)
    let delegate = Delegate(["issue": issues, "comment": comments])
    let view = ArrayView(
      try buildPipeline(query.ast, delegate, queryID: "q"), format: query.format, complete: true)
    var notified = 0
    _ = view.addListener { _, _ in notified += 1 }

    func titles() -> [String] { view.data.entries.map { $0.row["title"]!.stringValue! } }
    #expect(titles() == ["a"])

    // A comment on i1 makes it pass the EXISTS; it sorts after i2.
    comments.push(.add(["id": "c2", "issueID": "i1", "body": "y"]))
    view.flush()
    #expect(titles() == ["a", "b"])

    // i3 gets a comment, but the limit of 2 keeps it out.
    comments.push(.add(["id": "c3", "issueID": "i3", "body": "z"]))
    comments.push(.add(["id": "c4", "issueID": "i2", "body": "w"]))
    view.flush()
    #expect(titles() == ["a", "b"])
    let i2Comments = view.data.entries[0].relationships["comments"]!.entries
    #expect(i2Comments.map { $0.row["id"]!.stringValue! } == ["c4", "c1"])

    // Removing i2's comments drops it and lets i3 in.
    comments.push(.remove(["id": "c1", "issueID": "i2", "body": "x"]))
    comments.push(.remove(["id": "c4", "issueID": "i2", "body": "w"]))
    view.flush()
    #expect(titles() == ["b", "c"])
    #expect(notified > 0)
  }

  /// Rows in a table's layout and rows in their own read the same.
  @Test func rowsReadTheSameInAnyLayout() throws {
    let comments = MemorySource(Self.schema.tables["comment"]!)
    comments.push(.add(["body": "x", "id": "c1", "issueID": "i1"]))
    comments.push(.add(["id": "c2", "issueID": "i1", "body": "y"]))
    var c1 = comments.data.values().first { $0["id"] == "c1" }!
    let c2 = comments.data.values().first { $0["id"] == "c2" }!

    // A column the layout lacks is the row's own.
    c1["extra"] = 1
    #expect(c1["extra"] == 1)
    #expect(c2["extra"] == nil)
    #expect(c1.keys == ["id", "issueID", "body", "extra"])

    // Equal, with equal hashes, whatever the layout or the column order.
    let own: Row = ["extra": 1, "issueID": "i1", "body": "x", "id": "c1"]
    #expect(own == c1)
    #expect(own.hashValue == c1.hashValue)
    #expect(own != c2)

    // A comparator for the source's layout reads rows in others by name.
    let compare = makeComparator([AST.OrderPart("body", .desc)], layout: comments.layout)
    #expect(compare(c2, own) < 0)
    #expect(compare(own, c2) > 0)

    // A generated `init(entry:)` whose index is off reads the column by name.
    let entry = ViewEntry(row: c1)
    let body: String = try entry.column("body", 0)
    let extra: Double = try entry.column("extra", 7)
    #expect(body == "x")
    #expect(extra == 1)
  }
}
