import Foundation
import Testing

@testable import Zero

/// The key-path builder over the generated row types
/// (`Generated/TestSchema.swift`, from the zql test schema).
@Suite struct TypedQueryTests {
  let base = AnyQuery(.test, "issue")

  @Test func buildsTheSameQueriesAsTheUntypedBuilder() {
    let cases: [(Query<Issue, [Issue]>, AnyQuery)] = [
      (zql.issue.where(\.closed, false), base.where("closed", false)),
      (zql.issue.where(\.createdAt, .gt, 3), base.where("createdAt", .gt, 3)),
      (
        zql.issue.where(\.ownerId, .in, ["u1", "u2"]),
        base.where("ownerId", .in, ["u1", "u2"])
      ),
      (
        zql.issue.where { $0.createdAt >= 2 && ($0.title.like("%a%") || !($0.closed == true)) },
        base.where { eb in
          eb.and(
            eb.cmp("createdAt", .ge, 2),
            eb.or(eb.cmp("title", .like, "%a%"), eb.not(eb.cmp("closed", .eq, true))))
        }
      ),
      (
        zql.issue.whereExists(\.comments) { $0.where(\.text, "x") },
        base.whereExists("comments") { $0.where("text", "x") }
      ),
      (
        zql.issue.where { $0.exists(\.owner, flip: true) || $0.exists(\.labels) },
        base.where { $0.or($0.exists("owner", flip: true), $0.exists("labels")) }
      ),
      (
        zql.issue.related(\.comments) { $0.orderBy(\.createdAt, .desc).limit(2) }
          .related(\.owner).related(\.labels) { $0.where(\.name, "bug") },
        base.related("comments") { $0.orderBy("createdAt", .desc).limit(2) }
          .related("owner").related("labels") { $0.where("name", "bug") }
      ),
      (
        zql.issue.related(\.oneComment) { $0.one() },
        base.related("oneComment") { $0.one() }
      ),
      (
        zql.issue.orderBy(\.title, .asc).limit(10)
          .start(Issue(id: "i1", title: "t", description: "", closed: false, createdAt: 1)),
        base.orderBy("title", .asc).limit(10)
          .start(
            [
              "id": "i1", "title": "t", "description": "", "closed": false, "ownerId": nil,
              "createdAt": 1,
            ])
      ),
    ]
    for (typed, untyped) in cases {
      #expect(typed.ast == untyped.ast)
      #expect(typed.format == untyped.format)
      #expect(typed.hash == untyped.hash)
    }
    #expect(zql.issue.one().ast == base.one().ast)
    #expect(zql.issue.one().format.singular)
  }

  /// `= NULL` matches nothing, so comparing with nil uses `IS`.
  @Test func comparingWithNilUsesIs() {
    #expect(zql.issue.where(\.ownerId, nil).ast == base.where("ownerId", .is, nil).ast)
    #expect(zql.issue.where { $0.ownerId != nil }.ast == base.where("ownerId", .isNot, nil).ast)
    #expect(zql.issue.where(\.ownerId, .eq, nil).ast == base.where("ownerId", .eq, nil).ast)
  }

  final class Delegate: BuilderDelegate {
    let sources: [String: MemorySource]
    init(_ sources: [String: MemorySource]) { self.sources = sources }
    func getSource(_ tableName: String) -> (any Source)? { sources[tableName] }
  }

  func materialize<R: QueryResult>(_ query: Query<R.Row, R>, _ data: [String: [Row]]) throws -> R {
    var sources: [String: MemorySource] = [:]
    for name in ZeroSchema.test.tableNames {
      let source = MemorySource(ZeroSchema.test.tables[name]!)
      for row in data[name] ?? [] { source.push(.add(row)) }
      sources[name] = source
    }
    let view = ArrayView(
      try buildPipeline(query.ast, Delegate(sources), queryID: "q"), format: query.format,
      complete: true)
    return try R(view: view.data)
  }

  let data: [String: [Row]] = [
    "issue": [
      [
        "id": "i1", "title": "A", "description": "", "closed": false, "ownerId": "u1",
        "createdAt": 1,
      ],
      [
        "id": "i2", "title": "B", "description": "", "closed": true, "ownerId": nil,
        "createdAt": 2,
      ],
    ],
    "user": [
      [
        "id": "u1", "name": "Alice",
        "metadata": ["registrar": "github", "email": "a@example.com", "altContacts": ["b"]],
      ]
    ],
    "comment": [
      ["id": "c1", "authorId": "u1", "issueId": "i1", "text": "x", "createdAt": 5],
      ["id": "c2", "authorId": "u1", "issueId": "i1", "text": "y", "createdAt": 6],
    ],
    "label": [["id": "l1", "name": "bug"]],
    "issueLabel": [["issueId": "i1", "labelId": "l1"]],
  ]

  @Test func decodesViewsIntoRowStructs() throws {
    let issues = try materialize(
      zql.issue.related(\.owner).related(\.labels)
        .related(\.comments) { $0.orderBy(\.createdAt, .desc) }
        .orderBy(\.createdAt, .asc),
      data)
    #expect(issues.map(\.id) == ["i1", "i2"])
    let i1 = issues[0]
    #expect(i1.owner?.name == "Alice")
    #expect(i1.owner?.metadata?.registrar == .github)
    #expect(i1.owner?.metadata?.altContacts == ["b"])
    #expect(i1.comments?.map(\.id) == ["c2", "c1"])
    #expect(i1.labels?.map(\.name) == ["bug"])
    #expect(issues[1].owner == nil)
    #expect(issues[1].ownerId == nil)
    #expect(issues[1].comments == [])
    // Relationships the query didn't ask for stay nil.
    #expect(i1.oneComment == nil && i1.ownerComments == nil)

    let one = try materialize(zql.issue.where(\.id, "i2").one(), data)
    #expect(one?.title == "B")
    #expect(try materialize(zql.issue.where(\.id, "nope").one(), data) == nil)
  }

  @Test func rowsRoundTripThroughCodable() throws {
    let issue = try materialize(zql.issue.related(\.owner).related(\.comments), data)[0]
    let decoded = try JSONDecoder().decode(Issue.self, from: JSONEncoder().encode(issue))
    #expect(decoded == issue)
    #expect(decoded.owner?.metadata?.email == "a@example.com")
    // A row encoded without its relationships decodes without them.
    let bare = try JSONDecoder().decode(
      Issue.self,
      from: Data(
        #"{"id":"i9","title":"t","description":"","closed":false,"createdAt":1}"#.utf8))
    #expect(bare.owner == nil && bare.comments == nil && bare.ownerId == nil)
  }

  @Test func readsANullInAColumnThatCannotBeNullAsZero() throws {
    let issues = try materialize(
      zql.issue,
      ["issue": [["id": "i1", "title": nil, "description": nil, "closed": nil, "createdAt": nil]]])
    #expect(issues.map(\.title) == [""])
    #expect(issues.map(\.closed) == [false])
    #expect(issues.map(\.createdAt) == [0])
    #expect(issues.map(\.ownerId) == [nil])
    // An enum has no zero to read a null as.
    #expect(FxStatus.valueForNull == nil)
  }

  @Test func rejectsValuesTheRowTypesCannotHold() throws {
    #expect(throws: ZeroValueError.self) {
      try FxStatus(zeroValue: "reopened")
    }
    #expect(throws: ZeroValueError.self) {
      try Int(zeroValue: 1.5)
    }
    #expect(throws: ZeroValueError.self) {
      try FxSettings(zeroValue: ["theme": "light", "fontSize": 12])
    }
    let settings = FxSettings(theme: .dark, fontSize: 12, tags: ["a"], extra: nil)
    #expect(settings.zeroValue == ["theme": "dark", "fontSize": 12, "tags": ["a"], "extra": nil])
    #expect((try? FxSettings(zeroValue: settings.zeroValue)) == settings)
    #expect(FxStatus.inProgress.rawValue == "in-progress")
  }
}
