import Foundation
import Testing

@testable import Zero

/// Edge cases of the JSON layer, the wire messages, and the query hashes,
/// against the `sweep-*` vectors of
/// `packages/zero-conformance/src/primitives/sweep-wire.test.ts`.
///
/// Strings are compared by UTF-16 code unit: Swift's `==` on `String` is
/// canonical equivalence, under which `"é"` and `"e\u{301}"` are equal, and
/// JS tells them apart.
@Suite(.enabled(if: Vectors.available))
struct SweepWireTests {
  static func same(_ a: String, _ b: String) -> Bool {
    Array(a.utf16) == Array(b.utf16)
  }

  /// `JSON.stringify` with object keys sorted by UTF-16 code unit, as the
  /// generator's `sortedStringify` writes it.
  static func sortedStringify(_ v: JSONValue) -> String {
    switch v {
    case .array(let a):
      return "[" + a.map(sortedStringify).joined(separator: ",") + "]"
    case .object(let o):
      let keys = o.keys.sorted { Array($0.utf16).lexicographicallyPrecedes(Array($1.utf16)) }
      return "{"
        + keys.map { JSONSerializer.stringify(.string($0)) + ":" + sortedStringify(o[$0]!) }
        .joined(separator: ",") + "}"
    default:
      return JSONSerializer.stringify(v)
    }
  }

  // MARK: - JSON

  /// `JSON.parse` acceptance, and `JSON.stringify` of what it parsed, keys
  /// in JS property order, for each input.
  @Test func json() throws {
    for c in try Vectors.cases("sweep-json") {
      let input = c["input"]!.stringValue!
      let ok = c["ok"]!.boolValue!
      let value: JSONValue
      do {
        value = try JSONParser.parse(input)
      } catch {
        #expect(!ok, "rejected \(String(reflecting: input)): \(error)")
        continue
      }
      #expect(ok, "accepted \(String(reflecting: input))")
      guard ok else { continue }
      let expected = c["stringify"]!.stringValue!
      let actual = JSONSerializer.stringify(value)
      #expect(
        Self.same(actual, expected),
        "\(String(reflecting: input)): Swift \(actual.debugDescription), JS \(expected.debugDescription)"
      )
    }
  }

  /// JSON objects whose keys are canonically equivalent but different
  /// strings have one property per key in JS (`JSON.parse('{"\u00e9":1,
  /// "e\u0301":2}')` has two). A `[String: JSONValue]` would merge them:
  /// it hashes and compares keys by canonical equivalence.
  @Test func canonicallyEquivalentKeysStayApart() throws {
    let value = try JSONParser.parse(#"{"\u00e9":1,"e\u0301":2}"#)
    #expect(value.objectValue?.count == 2, "\(value)")
    // The two are different values, as they are in JS.
    #expect(JSONValue.object(["\u{e9}": 1]) != JSONValue.object(["e\u{301}": 1]))
  }

  /// A row value nested as deep as the parser accepts: the poke takes 5
  /// levels, the value the rest of `JSONParser.maxDepth`. Parsing, encoding,
  /// hashing, and comparing it must not overflow the stack of the thread it
  /// is on: here one with 512 KB, the size of a test or cooperative thread
  /// on macOS. One level more is rejected. (zero-cache writes about 4,100
  /// levels; see JSParity.md.)
  @Test func deepRowValue() async throws {
    func poke(_ depth: Int) -> String {
      #"["pokePart",{"pokeID":"p","rowsPatch":[{"op":"put","tableName":"t","value":{"id":"x","j":"#
        + String(repeating: "[", count: depth) + String(repeating: "]", count: depth) + "}}]}]"
    }
    let text = poke(JSONParser.maxDepth - 5)
    #expect(throws: (any Error).self) { try Downstream(text: poke(JSONParser.maxDepth - 4)) }
    let messages = try await onThread(stackSize: 512 << 10) {
      let message = try Downstream(text: text)
      let encoded = JSONSerializer.stringify(message.json)
      let decoded = try Downstream(text: encoded)
      var hasher = Hasher()
      message.hash(into: &hasher)
      _ = hasher.finalize()
      return Messages([message, decoded], encodedLength: encoded.utf8.count)
    }
    #expect(messages.encodedLength == text.utf8.count)
    #expect(messages.all[0] == messages.all[1])
    // Freeing still recurses, in `Array`'s deinit, and fits as well.
    try await onThread(stackSize: 512 << 10) { messages.all = [] }
  }

  final class Messages: @unchecked Sendable {
    var all: [Downstream]
    let encodedLength: Int

    init(_ all: [Downstream], encodedLength: Int) {
      self.all = all
      self.encodedLength = encodedLength
    }
  }

  /// Runs `body` on a thread with a stack of `stackSize` bytes.
  private func onThread<T: Sendable>(
    stackSize: Int, _ body: @escaping @Sendable () throws -> T
  ) async throws -> T {
    try await withCheckedThrowingContinuation { (done: CheckedContinuation<T, any Error>) in
      let thread = Thread { done.resume(with: Result { try body() }) }
      thread.stackSize = stackSize
      thread.start()
    }
  }

  // MARK: - Hashes

  @Test func nameAndArgsHash() throws {
    for c in try Vectors.cases("sweep-name-args-hash") {
      let name = c["name"]!.stringValue!
      let argsText = c["args"]!.stringValue!
      let args = try JSONParser.parse(argsText).arrayValue!
      let expected = c["hash"]!.stringValue!
      let actual = QueryHash.hashOfNameAndArgs(name, args)
      #expect(
        actual == expected,
        "\(String(reflecting: name)) \(argsText): Swift \(actual), JS \(expected)")
    }
  }

  /// `QueryImpl.hash()` of queries built with the query builder: the AST,
  /// the format (`one()` versus `limit(1)`), the system, the junction of a
  /// two-hop relationship, and a custom query's name and arguments.
  @Test func queryHash() throws {
    for c in try Vectors.cases("sweep-query-hash") {
      let spec = c["query"]!
      let system = AST.System(rawValue: spec["system"]?.stringValue ?? "client")!
      let root = AnyQuery(.test, spec["table"]!.stringValue!, system: system)
      let query = Self.run(root, spec["ops"]!.arrayValue!)
      let expected = c["hash"]!.stringValue!
      #expect(
        query.hash == expected,
        """
        \(spec): Swift \(query.hash), JS \(expected)
          Swift AST \(query.ast.json)
          JS AST    \(c["ast"]!)
        """)
    }
  }

  static func run(_ query: AnyQuery, _ ops: [JSONValue]) -> AnyQuery {
    var q = query
    for op in ops {
      let a = op.arrayValue!
      switch a[0].stringValue! {
      case "where":
        q = q.where(a[1].stringValue!, AST.SimpleOperator(rawValue: a[2].stringValue!)!, a[3])
      case "whereExpr":
        let e = a[1]
        q = q.where { expression($0, e) }
      case "whereExists":
        let sub = a[3].arrayValue!
        q = q.whereExists(
          a[1].stringValue!, flip: a[2]["flip"]?.boolValue, scalar: a[2]["scalar"]?.boolValue
        ) { run($0, sub) }
      case "related":
        let sub = a[2].arrayValue!
        q = q.related(a[1].stringValue!) { run($0, sub) }
      case "one":
        q = q.one()
      case "limit":
        q = q.limit(Int(a[1].numberValue!))
      case "orderBy":
        q = q.orderBy(a[1].stringValue!, AST.Direction(rawValue: a[2].stringValue!)!)
      case "start":
        q = q.start(a[1].objectValue!, inclusive: a[2].boolValue!)
      case "nameAndArgs":
        q = q.nameAndArgs(a[1].stringValue!, a[2].arrayValue!)
      case let other:
        preconditionFailure("unknown op \(other)")
      }
    }
    return q
  }

  static func expression(_ eb: ExpressionBuilder, _ e: JSONValue) -> AST.Condition {
    if let cmp = e["cmp"]?.arrayValue {
      return eb.cmp(cmp[0].stringValue!, AST.SimpleOperator(rawValue: cmp[1].stringValue!)!, cmp[2])
    }
    if let lit = e["cmpLit"]?.arrayValue {
      return eb.cmpLit(lit[0], AST.SimpleOperator(rawValue: lit[1].stringValue!)!, lit[2])
    }
    if let and = e["and"]?.arrayValue {
      return eb.and(and.map { expression(eb, $0) })
    }
    if let or = e["or"]?.arrayValue {
      return eb.or(or.map { expression(eb, $0) })
    }
    if let not = e["not"] {
      return eb.not(expression(eb, not))
    }
    let exists = e["exists"]!.arrayValue!
    let sub = exists[2].arrayValue!
    return eb.exists(
      exists[0].stringValue!, flip: exists[1]["flip"]?.boolValue,
      scalar: exists[1]["scalar"]?.boolValue
    ) { run($0, sub) }
  }

  // MARK: - Protocol

  /// Downstream messages with keys JS keeps apart: rows, primary keys,
  /// merges, client IDs, and `__proto__`. Compared with keys sorted: Swift
  /// re-encodes a message from its fields, in an order of its own.
  @Test func downstream() throws {
    for c in try Vectors.cases("sweep-downstream") {
      let text = c["text"]!.stringValue!
      let expected = c["output"]!.stringValue!
      do {
        let message = try Downstream(text: text)
        let actual = Self.sortedStringify(message.json)
        #expect(
          Self.same(actual, expected),
          "\(text): Swift \(actual.debugDescription), JS \(expected.debugDescription)")
      } catch {
        Issue.record("rejected \(text): \(error)")
      }
    }
  }
}
