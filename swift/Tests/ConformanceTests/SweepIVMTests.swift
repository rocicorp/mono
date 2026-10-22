import Testing

@testable import Zero

/// Replays the adversarial IVM vectors from
/// `packages/zero-conformance/src/ivm/sweep-ivm.test.ts` (written to
/// `generated/sweep-ivm-*.json`) and a few hand-checked cases whose expected
/// values come from the JS implementation.
@Suite(.enabled(if: Vectors.available))
struct SweepIVMTests {
  static func runAll(_ file: JSONValue, maxReported: Int = 10) throws {
    let shared = file["shared"]!
    let schema = try ZeroSchema(json: shared["schema"]!)
    let datasets = shared["datasets"]!.arrayValue!
    var failures = 0
    for c in file["cases"]!.arrayValue! {
      if let mismatch = try IVMConformanceTests.run(c, schema: schema, datasets: datasets) {
        failures += 1
        if failures <= maxReported {
          Issue.record("\(c["name"]!.stringValue!): \(c["program"]!)\n\(mismatch)")
        }
      }
    }
    #expect(failures == 0, "\(failures) cases failed")
  }

  @Test(.enabled(if: Vectors.generatedAvailable)) func sweepCurated() throws {
    try Self.runAll(try Vectors.generatedFile("sweep-ivm-curated"))
  }

  @Test(.enabled(if: Vectors.generatedAvailable)) func sweepFuzz() throws {
    try Self.runAll(try Vectors.generatedFile("sweep-ivm-fuzz"))
  }
}

/// Range filters over a NaN column value. Expected values come from running
/// the JS pipeline (`buildPipeline` + `MemorySource`) on the same rows: JS
/// `compareValues` returns `a - b`, which is NaN for a NaN operand, and every
/// comparison of NaN with 0 is false, so a NaN row never passes a range
/// filter and a NaN bound matches no row.
@Suite struct SweepIVMNaNTests {
  static let schema = ZeroSchema(
    tables: [
      TableSchema(
        name: "issue",
        columns: [("id", ColumnSchema(.string)), ("createdAt", ColumnSchema(.number))],
        primaryKey: ["id"])
    ],
    relationships: [:])

  final class Delegate: BuilderDelegate {
    let sources: [String: MemorySource]
    init(_ sources: [String: MemorySource]) { self.sources = sources }
    func getSource(_ tableName: String) -> (any Source)? { sources[tableName] }
  }

  static func ids(_ op: AST.SimpleOperator, _ value: JSONValue) throws -> [String] {
    let issues = MemorySource(schema.tables["issue"]!)
    issues.push(.add(["id": "i1", "createdAt": .number(1)]))
    issues.push(.add(["id": "i2", "createdAt": .number(.nan)]))
    let query = AnyQuery(schema, "issue").where("createdAt", op, value)
    let view = ArrayView(
      try buildPipeline(query.ast, Delegate(["issue": issues]), queryID: "q"),
      format: query.format, complete: true)
    return view.data.entries.map { $0.row["id"]!.stringValue! }
  }

  // JS: < NaN [], <= NaN [], > NaN [], >= NaN [], < 1 [], <= 1 [i1],
  // > 1 [], >= 1 [i1]; = NaN [], != NaN [i1, i2], IN [NaN] [i2].
  @Test(arguments: [
    (AST.SimpleOperator.lt, JSONValue.number(.nan), [String]()),
    (.le, .number(.nan), []),
    (.gt, .number(.nan), []),
    (.ge, .number(.nan), []),
    (.lt, .number(1), []),
    (.le, .number(1), ["i1"]),
    (.gt, .number(1), []),
    (.ge, .number(1), ["i1"]),
    (.eq, .number(.nan), []),
    (.ne, .number(.nan), ["i1", "i2"]),
    (.in, .array([.number(.nan)]), ["i2"]),
  ])
  func nanRangeFilter(_ op: AST.SimpleOperator, _ value: JSONValue, _ expected: [String]) throws {
    #expect(try Self.ids(op, value) == expected, "\(op.rawValue) \(value)")
  }
}
