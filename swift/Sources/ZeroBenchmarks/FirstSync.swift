import Foundation
import Zero

/// zbugs' first sync: the tables its issue list reads, with the
/// relationships of `apps/zbugs/shared/schema.ts`, and the rows of one
/// poke as zero-cache sent them to the zbugs-swift app.
enum FirstSync {
  /// The rows of the profiled first sync, by table.
  static let counts = [
    "issue": 516, "comment": 894, "issueLabel": 603, "label": 30, "user": 14, "project": 1,
  ]
  /// How many parts the poke came in.
  static let parts = 21
  static let pageSize = 50
  static let userID = "user0"

  static let schema = ZeroSchema(tables: tables, relationships: relationships)

  private static let tables: [TableSchema] = [
    TableSchema(
      name: "user",
      columns: [
        ("id", ColumnSchema(.string)),
        ("login", ColumnSchema(.string)),
        ("name", ColumnSchema(.string, optional: true)),
        ("avatar", ColumnSchema(.string)),
        ("role", ColumnSchema(.string)),
      ],
      primaryKey: ["id"], uniqueKeys: [["login"]]),
    TableSchema(
      name: "project",
      columns: [
        ("id", ColumnSchema(.string)),
        ("name", ColumnSchema(.string)),
        ("lowerCaseName", ColumnSchema(.string)),
        ("issueCountEstimate", ColumnSchema(.number, optional: true)),
        ("supportsSearch", ColumnSchema(.boolean)),
        ("markURL", ColumnSchema(.string, optional: true)),
        ("logoURL", ColumnSchema(.string, optional: true)),
      ],
      primaryKey: ["id"], uniqueKeys: [["name"], ["lowerCaseName"]]),
    TableSchema(
      name: "issue",
      columns: [
        ("id", ColumnSchema(.string)),
        ("shortID", ColumnSchema(.number, optional: true)),
        ("title", ColumnSchema(.string)),
        ("open", ColumnSchema(.boolean)),
        ("modified", ColumnSchema(.number)),
        ("created", ColumnSchema(.number)),
        ("projectID", ColumnSchema(.string)),
        ("creatorID", ColumnSchema(.string)),
        ("assigneeID", ColumnSchema(.string, optional: true)),
        ("description", ColumnSchema(.string)),
        ("visibility", ColumnSchema(.string)),
      ],
      primaryKey: ["id"]),
    TableSchema(
      name: "comment",
      columns: [
        ("id", ColumnSchema(.string)),
        ("issueID", ColumnSchema(.string)),
        ("created", ColumnSchema(.number)),
        ("body", ColumnSchema(.string)),
        ("creatorID", ColumnSchema(.string)),
      ],
      primaryKey: ["id"]),
    TableSchema(
      name: "label",
      columns: [
        ("id", ColumnSchema(.string)),
        ("name", ColumnSchema(.string)),
        ("projectID", ColumnSchema(.string)),
      ],
      primaryKey: ["id"], uniqueKeys: [["projectID", "name"]]),
    TableSchema(
      name: "issueLabel",
      columns: [
        ("issueID", ColumnSchema(.string)),
        ("labelID", ColumnSchema(.string)),
        ("projectID", ColumnSchema(.string)),
      ],
      primaryKey: ["labelID", "issueID"]),
    TableSchema(
      name: "viewState",
      columns: [
        ("issueID", ColumnSchema(.string)),
        ("userID", ColumnSchema(.string)),
        ("viewed", ColumnSchema(.number)),
      ],
      primaryKey: ["userID", "issueID"]),
  ]

  private static let relationships: [String: [String: [RelationshipHop]]] = [
    "user": [
      "createdIssues": [hop(["id"], ["creatorID"], "issue", .many)],
      "assignedIssues": [hop(["id"], ["assigneeID"], "issue", .many)],
    ],
    "project": [
      "issues": [hop(["id"], ["projectID"], "issue", .many)],
      "labels": [hop(["id"], ["projectID"], "label", .many)],
    ],
    "issue": [
      "project": [hop(["projectID"], ["id"], "project", .one)],
      "issueLabels": [hop(["id"], ["issueID"], "issueLabel", .many)],
      "labels": [
        hop(["id"], ["issueID"], "issueLabel", .many),
        hop(["labelID"], ["id"], "label", .many),
      ],
      "comments": [hop(["id"], ["issueID"], "comment", .many)],
      "creator": [hop(["creatorID"], ["id"], "user", .one)],
      "assignee": [hop(["assigneeID"], ["id"], "user", .one)],
      "viewState": [hop(["id"], ["issueID"], "viewState", .many)],
    ],
    "comment": [
      "creator": [hop(["creatorID"], ["id"], "user", .one)],
      "issue": [hop(["issueID"], ["id"], "issue", .one)],
    ],
    "issueLabel": [
      "issue": [hop(["issueID"], ["id"], "issue", .one)],
      "label": [hop(["labelID"], ["id"], "label", .one)],
    ],
    "label": [
      "project": [hop(["projectID"], ["id"], "project", .one)]
    ],
  ]

  private static func hop(
    _ source: [String], _ dest: [String], _ table: String, _ cardinality: Cardinality
  ) -> RelationshipHop {
    RelationshipHop(
      sourceField: source, destField: dest, destSchema: table, cardinality: cardinality)
  }

  /// The first page of zbugs' issue list as the app opens it: `issueListV2`
  /// (`buildListQuery` in `apps/zbugs/shared/queries.ts`) for the open
  /// issues of project Zero, newest change first, signed in but not crew.
  static var issueList: AnyQuery {
    AnyQuery(schema, "issue")
      .related("viewState") { $0.where("userID", .string(userID)).one() }
      .related("labels")
      .related("creator")
      .whereExists("project", scalar: true) { $0.where("lowerCaseName", "zero") }
      .orderBy("modified", .desc)
      .orderBy("id", .desc)
      .limit(pageSize + 1)
      .where { $0.and($0.cmp("open", .bool(true))) }
      .where("visibility", "public")
  }

  /// The `rowsPatch` puts of the poke, by table, with made-up but
  /// zbugs-like contents: nanoid keys, titles, Markdown bodies, and
  /// millisecond timestamps.
  static func rows() -> [String] {
    var random = SplitMix64(seed: 2064)
    func id() -> String { random.nanoid() }
    func text(_ words: ClosedRange<Int>) -> String { random.sentence(words) }
    let now = 1_760_000_000_000.0

    let projectID = id()
    var out: [String] = []
    func put(_ table: String, _ value: [String: JSONValue]) {
      out.append(
        #"{"op":"put","tableName":"\#(table)","value":"#
          + JSONSerializer.stringify(.object(JSONObject(value))) + "}")
    }

    put(
      "project",
      [
        "id": .string(projectID), "name": "Zero", "lowerCaseName": "zero",
        "issueCountEstimate": .number(Double(counts["issue"]!)), "supportsSearch": true,
        "markURL": .null, "logoURL": .null,
      ])

    var users: [String] = []
    for n in 0..<counts["user"]! {
      let uid = n == 0 ? userID : id()
      users.append(uid)
      let login = "\(random.word())\(random.word())\(n)"
      put(
        "user",
        [
          "id": .string(uid), "login": .string(login),
          "name": n % 3 == 0 ? .null : .string(text(2...2).capitalized),
          "avatar": .string("https://avatars.githubusercontent.com/u/\(1_000_000 + n * 7919)?v=4"),
          "role": .string(n < 5 ? "crew" : "user"),
        ])
    }

    var labels: [String] = []
    for n in 0..<counts["label"]! {
      let lid = id()
      labels.append(lid)
      put(
        "label",
        [
          "id": .string(lid), "name": .string("\(random.word())-\(n)"),
          "projectID": .string(projectID),
        ])
    }

    var issues: [(id: String, created: Double)] = []
    for n in 0..<counts["issue"]! {
      let iid = id()
      let created = now - Double(random.next(below: 400 * 86_400)) * 1000
      let modified = created + Double(random.next(below: 30 * 86_400)) * 1000
      issues.append((iid, created))
      put(
        "issue",
        [
          "id": .string(iid), "shortID": .number(Double(3000 + n)),
          "title": .string(text(4...12).capitalizedFirst),
          "open": .bool(random.next(below: 10) < 6),
          "modified": .number(modified), "created": .number(created),
          "projectID": .string(projectID),
          "creatorID": .string(users[random.next(below: users.count)]),
          "assigneeID": random.next(below: 2) == 0
            ? .null : .string(users[random.next(below: 5)]),
          "description": .string(random.paragraphs(0...4)),
          "visibility": .string(random.next(below: 20) == 0 ? "internal" : "public"),
        ])
    }

    for _ in 0..<counts["comment"]! {
      let issue = issues[random.next(below: issues.count)]
      put(
        "comment",
        [
          "id": .string(id()), "issueID": .string(issue.id),
          "created": .number(issue.created + Double(random.next(below: 60 * 86_400)) * 1000),
          "body": .string(random.paragraphs(1...3)),
          "creatorID": .string(users[random.next(below: users.count)]),
        ])
    }

    // Distinct (label, issue) pairs.
    var pairs = Set<String>()
    while pairs.count < counts["issueLabel"]! {
      let issue = issues[random.next(below: issues.count)].id
      let label = labels[random.next(below: labels.count)]
      guard pairs.insert(label + "/" + issue).inserted else { continue }
      put(
        "issueLabel",
        ["issueID": .string(issue), "labelID": .string(label), "projectID": .string(projectID)])
    }
    return out
  }
}

/// A deterministic generator, so every run syncs the same rows.
struct SplitMix64 {
  private var state: UInt64
  init(seed: UInt64) { state = seed }

  mutating func next() -> UInt64 {
    state &+= 0x9E37_79B9_7F4A_7C15
    var z = state
    z = (z ^ (z >> 30)) &* 0xBF58_476D_1CE4_E5B9
    z = (z ^ (z >> 27)) &* 0x94D0_49BB_1331_11EB
    return z ^ (z >> 31)
  }

  mutating func next(below n: Int) -> Int { Int(next() % UInt64(n)) }

  private static let alphabet = Array(
    "useandom-26T198340PX75pxJACKVERYMINDBUSHWOLF_GQZbfghjklqvwyzrict")

  mutating func nanoid() -> String {
    String((0..<21).map { _ in Self.alphabet[next(below: 64)] })
  }

  private static let words = [
    "query", "client", "sync", "issue", "label", "cache", "replica", "server", "view", "row",
    "poke", "mutation", "schema", "index", "join", "filter", "order", "limit", "permission",
    "auth", "token", "socket", "reconnect", "error", "crash", "slow", "fast", "scroll", "list",
    "page", "update", "delete", "insert", "migration", "postgres", "sqlite", "the", "a", "when",
    "after", "before", "with", "without", "should", "does", "not", "work", "fails", "on",
    "first", "load", "build", "deploy", "test", "docs", "example", "support", "custom", "add",
  ]

  mutating func word() -> String { Self.words[next(below: Self.words.count)] }

  mutating func sentence(_ words: ClosedRange<Int>) -> String {
    let n = words.lowerBound + next(below: words.count)
    return (0..<n).map { _ in word() }.joined(separator: " ")
  }

  mutating func paragraphs(_ count: ClosedRange<Int>) -> String {
    let n = count.lowerBound + next(below: count.count)
    return (0..<n).map { _ in
      (0..<(1 + next(below: 4))).map { _ in sentence(5...20).capitalizedFirst + "." }
        .joined(separator: " ")
    }.joined(separator: "\n\n")
  }
}

extension String {
  fileprivate var capitalizedFirst: String { prefix(1).uppercased() + dropFirst() }
}
