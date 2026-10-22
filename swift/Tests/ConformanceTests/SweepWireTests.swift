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
  /// On an 8 MB stack, as `deepRowValue`: a vector nests 1,000 deep.
  @Test func json() async throws {
    let cases = try Vectors.cases("sweep-json")
    try await onLargeStack {
      for c in cases {
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

  /// A row value nested 4,000 deep, which `JSON.stringify` on zero-cache
  /// writes (Node's default stack takes about 4,100 levels) and the parser
  /// accepts (its limit is 5,000). Parsing, encoding, hashing, comparing,
  /// and freeing it must not overflow the stack of the thread it is on.
  /// It runs on a thread with an 8 MB stack, the size of the macOS main
  /// thread's: test threads get 512 KB there, and the debug build's frames
  /// are larger than release's.
  @Test func deepRowValue() async throws {
    try await onLargeStack {
      let depth = 4_000
      let text =
        #"["pokePart",{"pokeID":"p","rowsPatch":[{"op":"put","tableName":"t","value":{"id":"x","j":"#
        + String(repeating: "[", count: depth) + String(repeating: "]", count: depth) + "}}]}]"
      let message = try Downstream(text: text)
      let encoded = JSONSerializer.stringify(message.json)
      #expect(encoded.utf8.count == text.utf8.count)
      #expect(message == (try Downstream(text: encoded)))
      var hasher = Hasher()
      message.hash(into: &hasher)
      _ = hasher.finalize()
    }
  }

  private func onLargeStack(_ body: @escaping @Sendable () throws -> Void) async throws {
    try await withCheckedThrowingContinuation { (done: CheckedContinuation<Void, any Error>) in
      let thread = Thread {
        done.resume(with: Result { try body() })
      }
      thread.stackSize = 8 << 20
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
