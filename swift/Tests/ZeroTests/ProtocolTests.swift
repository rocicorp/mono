import Testing

@testable import Zero

/// The protocol's valita edge cases, for checkouts without the conformance
/// vectors.
struct ProtocolTests {
  @Test func messageRoundTripsThroughText() throws {
    let push = Upstream.push(
      PushBody(
        clientGroupID: "g",
        mutations: [
          .custom(
            CustomMutation(
              id: 1, clientID: "c", name: "issue.create", args: [["id": "a"]], timestamp: 5)),
          .crud(
            CRUDMutation(
              id: 2, clientID: "c",
              ops: [.delete(tableName: "issue", primaryKey: ["id"], value: ["id": "a"])],
              timestamp: 6)),
        ],
        pushVersion: 1, timestamp: 7, requestID: "r"))
    #expect(try Upstream(text: push.text) == push)
    #expect(push.json[1]?["mutations"]?[1]?["name"] == "_zero_crud")
  }

  @Test func upstreamIsAnOpenTaggedTupleUnion() throws {
    #expect(try Upstream(json: ["newMessage", 1, 2]) == .unrecognized(["newMessage", 1, 2]))
    #expect(try Upstream(json: ["ping", ["zExtra": 1]]) == .ping)
    #expect(throws: DecodingError.self) { try Upstream(json: [1, [:]]) }
    #expect(throws: DecodingError.self) { try Upstream(json: ["ping"]) }
    #expect(throws: DecodingError.self) { try Upstream(json: []) }
    #expect(throws: DecodingError.self) { try Downstream(json: ["newMessage", [:]]) }
  }

  @Test func clientASTsAreStrict() throws {
    func put(_ ast: JSONValue) -> JSONValue {
      ["changeDesiredQueries", ["desiredQueriesPatch": [["op": "put", "hash": "h", "ast": ast]]]]
    }
    _ = try Upstream(
      json: put(["table": "issue", "start": ["row": ["any": 1], "exclusive": true]]))
    #expect(throws: DecodingError.self) {
      try Upstream(json: put(["table": "issue", "zExtra": 1]))
    }
    #expect(throws: DecodingError.self) {
      try Upstream(
        json: put(["table": "issue", "where": ["type": "and", "conditions": [], "x": 1]]))
    }
    // zero-cache's own ASTs are not strict.
    let row: JSONValue = [
      "clientID": "c", "queryID": "q", "ast": ["table": "issue", "zExtra": 1], "name": nil,
      "args": nil, "got": true, "deleted": false, "ttl": 0, "inactivatedAt": nil, "rowCount": 0,
    ]
    let inspect = try Downstream(json: ["inspect", ["id": "1", "op": "queries", "value": [row]]])
    #expect(inspect.json[1]?["value"]?[0]?["ast"] == ["table": "issue"])
  }

  @Test func clientASTsAreAtMost50Deep() throws {
    func nested(_ n: Int) -> JSONValue {
      var ast: JSONValue = ["table": "issue"]
      for _ in 0..<n {
        ast = [
          "table": "issue",
          "related": [
            ["correlation": ["parentField": ["id"], "childField": ["id"]], "subquery": ast]
          ],
        ]
      }
      return [
        "changeDesiredQueries", ["desiredQueriesPatch": [["op": "put", "hash": "h", "ast": ast]]],
      ]
    }
    _ = try Upstream(json: nested(50))
    #expect(throws: DecodingError.self) { try Upstream(json: nested(51)) }
  }

  @Test func mutationResultsAreReadAsTheJSClientReadsThem() throws {
    func result(_ json: JSONValue) throws -> MutationResult { try MutationResult(json: json) }
    #expect(try result([:]) == .ok(data: nil))
    #expect(try result(["data": [1]]) == .ok(data: [1]))
    #expect(try result(["error": "app", "message": "m"]) == .error(.app(message: "m")))
    // Any object is a result; with `data` it is OK, otherwise `error` makes it an error.
    #expect(try result(["error": "app", "data": 1]) == .ok(data: 1))
    #expect(try result(["error": 5]) == .error(MutationError(error: 5)))
    #expect(throws: DecodingError.self) { try result([]) }
  }

  @Test func pushResponseTriesTheOKVariantFirst() throws {
    #expect(
      try PushResponseBody(json: ["mutations": [], "error": "http", "status": 500, "details": "d"])
        == .ok(mutations: []))
    #expect(
      try PushResponseBody(json: ["mutations": "x", "error": "zeroPusher", "details": "d"])
        == .error(.zeroPusher(details: "d", mutationIDs: nil)))
  }

  @Test func errorBodyVariants() throws {
    let body: JSONValue = [
      "kind": "PushFailed", "message": "m", "mutationIDs": [], "origin": "zeroCache",
      "reason": "http",
    ]
    #expect(throws: DecodingError.self) { try ErrorBody(json: body) }
    var withStatus = body.objectValue!
    withStatus["status"] = 503
    #expect(
      try ErrorBody(json: .object(withStatus))
        == .pushFailed(
          PushFailedBody(message: "m", mutationIDs: [], cause: .http(status: 503, bodyPreview: nil))
        ))
    #expect(throws: DecodingError.self) {
      try ErrorBody(json: ["kind": "Rebalance", "message": "m", "origin": "server"])
    }
  }

  @Test func pokeChunksMaySplitUTF8Sequences() throws {
    let payload = Array(#"[{"pokeID":"中"}]"#.utf8)
    let split = payload.firstIndex(of: 0xE4)! + 1
    let messages = [[0] + payload[..<split], [0] + payload[split...]]
    let parts = try PokeChunk.decode(messages.map { try PokeChunk.payload(ofBinaryMessage: $0) })
    #expect(parts == [PokePartBody(pokeID: "中")])
    // TextDecoder drops a BOM; invalid UTF-8 and other message types are errors.
    #expect(try PokeChunk.decode([[0xEF, 0xBB, 0xBF] + Array("[]".utf8)]) == [])
    #expect(throws: DecodingError.self) { try PokeChunk.decode([[0x5B, 0xFF, 0x5D]]) }
    #expect(throws: DecodingError.self) { try PokeChunk.payload(ofBinaryMessage: [1]) }
  }

  @Test func secProtocols() throws {
    let body = InitConnectionBody(desiredQueriesPatch: [.clear], activeClients: ["c"])
    let encoded = SecProtocols.encode(initConnection: body, authToken: "tö")
    #expect(!encoded.contains(where: { "+/=".contains($0) }))
    #expect(
      try SecProtocols.decode(encoded)
        == ["initConnectionMessage": Upstream.initConnection(body).json, "authToken": "tö"])
    // `encodeSecProtocols(undefined, undefined)`, and atob's forgiving forms of it.
    for s in ["e30%3D", "e30", "e30%3d", "%20e30="] { #expect(try SecProtocols.decode(s) == [:]) }
    for s in ["e30==", "e3=0", "e30%", "a"] {
      #expect(throws: DecodingError.self) { try SecProtocols.decode(s) }
    }
  }

  @Test func normalizedClientSchemaSortsPrimaryKeysByUTF16() {
    let schema = ClientSchema(tables: [
      "t": .init(columns: [:], primaryKey: ["\u{E000}", "\u{1F600}", "b", "B"])
    ])
    #expect(schema.normalized().tables["t"]?.primaryKey == ["B", "b", "\u{1F600}", "\u{E000}"])
  }
}
