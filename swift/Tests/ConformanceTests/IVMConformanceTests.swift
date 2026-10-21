import Testing

@testable import Zero

/// Runs the IVM vectors: hydrate a view over the dataset, apply each push,
/// and compare the view and the changes it received with JS.
@Suite(.enabled(if: Vectors.available))
struct IVMConformanceTests {
  final class Delegate: BuilderDelegate {
    let sources: [String: MemorySource]
    init(_ sources: [String: MemorySource]) { self.sources = sources }
    func getSource(_ tableName: String) -> (any Source)? { sources[tableName] }
  }

  /// Records what the pipeline pushes to the view.
  final class Recorder: Operator {
    let input: any Input
    weak var output: (any Output)?
    var changes: [JSONValue] = []

    init(_ input: any Input) {
      self.input = input
      input.setOutput(self)
    }

    func setOutput(_ output: any Output) { self.output = output }
    func getSchema() -> SourceSchema { input.getSchema() }
    func destroy() { input.destroy() }
    func fetch(_ req: FetchRequest) -> NodeStream { input.fetch(req) }

    func push(_ change: Change, pusher: any InputBase) {
      changes.append(Self.record(change))
      output?.push(change, pusher: self)
    }

    func reconcile(pusher: any InputBase) { output?.reconcile(pusher: self) }

    static func record(_ change: Change) -> JSONValue {
      switch change {
      case .add(let n): ["type": "add", "row": JSONValue(n.row)]
      case .remove(let n): ["type": "remove", "row": JSONValue(n.row)]
      case .edit(let n, let o):
        ["type": "edit", "row": JSONValue(n.row), "oldRow": JSONValue(o.row)]
      case .child(let n, let c):
        [
          "type": "child", "row": JSONValue(n.row),
          "child": ["relationshipName": .string(c.relationshipName), "change": record(c.change)],
        ]
      }
    }
  }

  static func sourceChange(_ push: JSONValue) -> (String, SourceChange) {
    let table = push["table"]!.stringValue!
    let row = Row(push["row"]!.objectValue!)
    switch push["type"]!.stringValue! {
    case "add": return (table, .add(row))
    case "remove": return (table, .remove(row))
    default: return (table, .edit(row, oldRow: Row(push["oldRow"]!.objectValue!)))
    }
  }

  /// Replays a harness `Program` (`src/ivm/harness.ts`) with the Swift
  /// builder.
  static func build(_ schema: ZeroSchema, _ program: JSONValue) -> AnyQuery {
    apply(AnyQuery(schema, program["table"]!.stringValue!), program["ops"]!)
  }

  static func apply(_ q: AnyQuery, _ ops: JSONValue) -> AnyQuery {
    ops.arrayValue!.reduce(q) { q, op in apply(q, op: op) }
  }

  static func sub(_ ops: JSONValue?) -> ((AnyQuery) -> AnyQuery)? {
    guard let ops, ops != .null else { return nil }
    return { apply($0, ops) }
  }

  static func op(_ s: JSONValue?) -> AST.SimpleOperator {
    AST.SimpleOperator(rawValue: s!.stringValue!)!
  }

  static func apply(_ q: AnyQuery, op: JSONValue) -> AnyQuery {
    let flip = op["flip"]?.boolValue
    let scalar = op["scalar"]?.boolValue
    switch op["op"]!.stringValue! {
    case "where":
      let field = op["field"]!.stringValue!
      let value = op["value"] ?? .null
      return op["cmp"] == nil ? q.where(field, value) : q.where(field, Self.op(op["cmp"]), value)
    case "whereExpr":
      return q.where { expr($0, op["expr"]!) }
    case "whereExists":
      return q.whereExists(op["rel"]!.stringValue!, flip: flip, scalar: scalar, sub(op["sub"]))
    case "related":
      return q.related(op["rel"]!.stringValue!, sub(op["sub"]))
    case "orderBy":
      return q.orderBy(op["field"]!.stringValue!, AST.Direction(rawValue: op["dir"]!.stringValue!)!)
    case "limit":
      return q.limit(Int(op["n"]!.numberValue!))
    case "start":
      return q.start(op["row"]!.objectValue!, inclusive: op["inclusive"]?.boolValue ?? false)
    case "one":
      return q.one()
    case let other:
      fatalError("unknown op \(other)")
    }
  }

  static func expr(_ eb: ExpressionBuilder, _ e: JSONValue) -> AST.Condition {
    if let c = e["cmp"]?.arrayValue {
      return eb.cmp(c[0].stringValue!, op(c[1]), c[2])
    }
    if let c = e["and"]?.arrayValue { return eb.and(c.map { expr(eb, $0) }) }
    if let c = e["or"]?.arrayValue { return eb.or(c.map { expr(eb, $0) }) }
    if let c = e["not"] { return eb.not(expr(eb, c)) }
    return eb.exists(
      e["exists"]!.stringValue!, flip: e["flip"]?.boolValue, scalar: e["scalar"]?.boolValue,
      sub(e["sub"]))
  }

  /// Runs one case; returns a description of the first mismatch.
  static func run(_ c: JSONValue, schema: ZeroSchema, datasets: [JSONValue]) throws -> String? {
    let dataset = datasets[Int(c["dataset"]!.numberValue!)]
    var sources: [String: MemorySource] = [:]
    for name in schema.tableNames {
      let source = MemorySource(schema.tables[name]!)
      for row in dataset["data"]?[name]?.arrayValue ?? [] {
        source.push(.add(Row(row.objectValue!)))
      }
      sources[name] = source
    }
    let ast = try AST(json: c["ast"]!)
    let format = try Format(json: c["format"]!)
    let built = build(schema, c["program"]!)
    if built.ast != ast {
      return "builder ast\n  swift: \(built.ast.json)\n  js:    \(c["ast"]!)"
    }
    if built.format != format {
      return "builder format\n  swift: \(built.format.json)\n  js:    \(c["format"]!)"
    }
    let result = c["result"]!
    let delegate = Delegate(sources)
    let recorder: Recorder
    let view: ArrayView
    do {
      let input = try buildPipeline(ast, delegate, queryID: "q")
      recorder = Recorder(input)
      view = ArrayView(recorder, format: format, complete: true)
    } catch {
      if let expected = result["error"]?.stringValue {
        return error.description == expected ? nil : "error \(error) != \(expected)"
      }
      return "unexpected error \(error)"
    }
    if let expected = result["error"] {
      return "expected error \(expected)"
    }
    func viewJSON() -> JSONValue { view.data.json ?? .null }
    if viewJSON() != result["initial"]! {
      return "initial view\n  swift: \(viewJSON())\n  js:    \(result["initial"]!)"
    }
    let steps = result["steps"]!.arrayValue!
    for (i, push) in dataset["pushes"]!.arrayValue!.enumerated() {
      recorder.changes.removeAll()
      let (table, change) = sourceChange(push)
      sources[table]!.push(change)
      view.flush()
      let step = steps[i]
      if JSONValue.array(recorder.changes) != step["changes"]! {
        return
          "push \(i) \(push) changes\n  swift: \(JSONValue.array(recorder.changes))\n  js:    \(step["changes"]!)"
      }
      if viewJSON() != step["data"]! {
        return "push \(i) \(push) view\n  swift: \(viewJSON())\n  js:    \(step["data"]!)"
      }
    }
    return nil
  }

  static func runAll(_ file: JSONValue) throws {
    let shared = file["shared"]!
    let schema = try ZeroSchema(json: shared["schema"]!)
    let datasets = shared["datasets"]!.arrayValue!
    var failures = 0
    for c in file["cases"]!.arrayValue! {
      if let mismatch = try run(c, schema: schema, datasets: datasets) {
        failures += 1
        if failures <= 5 {
          Issue.record("\(c["name"]!.stringValue!): \(c["ast"]!)\n\(mismatch)")
        }
      }
    }
    #expect(failures == 0, "\(failures) cases failed")
  }

  @Test func curated() throws {
    try Self.runAll(try Vectors.file("ivm"))
  }

  @Test(.enabled(if: Vectors.generatedAvailable)) func fuzz() throws {
    try Self.runAll(try Vectors.generatedFile("ivm-fuzz"))
  }
}
