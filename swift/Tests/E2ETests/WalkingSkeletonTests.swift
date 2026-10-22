import Foundation
import Testing
import Zero

/// The end-to-end environment: `ZERO_E2E_URL` is the origin of a zero-cache
/// over the fixture database (`packages/zero-conformance/src/e2e/`).
enum E2E {
  static let url = ProcessInfo.processInfo.environment["ZERO_E2E_URL"]

  /// The zql test schema the fixture database is built from
  /// (`packages/zql/src/query/test/test-schemas.ts`).
  static let schema = ZeroSchema(
    tables: [
      TableSchema(
        name: "issue", serverName: "issues",
        columns: [
          ("id", ColumnSchema(.string)), ("title", ColumnSchema(.string)),
          ("description", ColumnSchema(.string)), ("closed", ColumnSchema(.boolean)),
          ("ownerId", ColumnSchema(.string, optional: true, serverName: "owner_id")),
          ("createdAt", ColumnSchema(.number)),
        ],
        primaryKey: ["id"]),
      TableSchema(
        name: "user", serverName: "users",
        columns: [
          ("id", ColumnSchema(.string)), ("name", ColumnSchema(.string)),
          ("metadata", ColumnSchema(.json, optional: true)),
        ],
        primaryKey: ["id"]),
      TableSchema(
        name: "comment", serverName: "comments",
        columns: [
          ("id", ColumnSchema(.string)), ("authorId", ColumnSchema(.string)),
          ("issueId", ColumnSchema(.string, serverName: "issue_id")),
          ("text", ColumnSchema(.string)), ("createdAt", ColumnSchema(.number)),
        ],
        primaryKey: ["id"]),
    ],
    relationships: [
      "issue": [
        "comments": [
          RelationshipHop(
            sourceField: ["id"], destField: ["issueId"], destSchema: "comment", cardinality: .many)
        ],
        "owner": [
          RelationshipHop(
            sourceField: ["ownerId"], destField: ["id"], destSchema: "user", cardinality: .one)
        ],
      ]
    ])
}

/// M1.5: connect to a real zero-cache, ask for a named query, and receive
/// its rows.
@Suite(.enabled(if: E2E.url != nil))
struct WalkingSkeletonTests {
  @Test func syncsTheRowsOfAQuery() async throws {
    // `issues.open` in packages/zero-conformance/src/e2e/app.ts. The server
    // runs its own definition; the client only names it.
    let hash = QueryHash.hashOfNameAndArgs("issues.open", [])
    let clientID = "swift-\(UInt32.random(in: 0...UInt32.max))"
    let initConnection = InitConnectionBody(
      desiredQueriesPatch: [
        .put(UpQueriesPut(hash: hash, ttl: 60_000, name: "issues.open", args: []))
      ],
      clientSchema: E2E.schema.clientSchema, activeClients: [clientID])
    let url = ConnectionURL(
      socketOrigin: E2E.url!, clientID: clientID, clientGroupID: "\(clientID)-group",
      timestamp: 0, lastMutationID: 0, wsid: "ws1", profileID: "p\(clientID)")
    let socket = try await NIOTransport().connect(
      url: url.string,
      secProtocol: SecProtocols.encode(initConnection: initConnection, authToken: nil))

    var connected = false
    var chunks: [ArraySlice<UInt8>] = []
    var rows: [String: [RowRecord]] = [:]
    var gotQueries: Set<String> = []
    receiving: while let message = try await socket.receive() {
      switch message {
      case .binary(let bytes):
        chunks.append(try PokeChunk.payload(ofBinaryMessage: bytes))
      case .text(let text):
        let downstream = try Downstream(text: text)
        switch downstream {
        case .connected:
          connected = true
        case .pokeStart:
          chunks = []
        case .pokePart(let part):
          apply([part])
        case .pokeEnd(let end):
          if end.cancel != true { apply(try PokeChunk.decode(chunks)) }
          if gotQueries.contains(hash) { break receiving }
        case .error(let error):
          Issue.record("zero-cache error: \(error)")
          break receiving
        default:
          break
        }
      }
    }
    await socket.close(code: 1000, reason: "done")

    func apply(_ parts: [PokePartBody]) {
      for part in parts {
        if case .some(let patch) = part.gotQueriesPatch {
          for op in patch {
            if case .put(let h, _) = op { gotQueries.insert(h) }
          }
        }
        for op in part.rowsPatch ?? [] {
          if case .put(let table, let value) = op { rows[table, default: []].append(value) }
        }
      }
    }

    #expect(connected)
    #expect(gotQueries.contains(hash))
    let toClient = NameMapper.serverToClient(E2E.schema)
    let issues = try (rows["issues"] ?? []).map { try toClient.row("issues", $0) }
    let comments = try (rows["comments"] ?? []).map { try toClient.row("comments", $0) }
    let users = try (rows["users"] ?? []).map { try toClient.row("users", $0) }
    for (table, rows) in rows.sorted(by: { $0.key < $1.key }) {
      for row in rows { print("[e2e] \(table) \(JSONSerializer.stringify(.object(row)))") }
    }
    // The fixture's open issues, their comments, and their owners.
    #expect(Set(issues.map { $0["id"]! }) == ["i1", "i3", "i4"])
    #expect(Set(comments.map { $0["id"]! }) == ["c1", "c2", "c3"])
    #expect(Set(users.map { $0["id"]! }) == ["u1"])
    #expect(issues.first { $0["id"] == "i3" }?["title"] == "Emoji 😀 in titles")
    #expect(issues.first { $0["id"] == "i1" }?["ownerId"] == "u1")
    #expect(users.first?["metadata"] == ["registrar": "github", "email": "alice@example.com"])
  }
}
