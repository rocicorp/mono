/// The drift harness: checks an app's Swift queries and mutators against
/// what their TS definitions did on the same samples
/// (`zero-swift drift record` writes the vectors).
///
/// - Every name in the TS registries has a Swift definition, and the other
///   way round.
/// - Each query sample's arguments read as the Swift argument type and
///   encode back to the same JSON, and its AST has the same hash.
/// - Each mutator sample's arguments round-trip the same way, and the
///   mutator, run on the sample's rows, has the same outcome and changes the
///   same rows.
@MainActor
public enum Drift {
  /// The differences between `vectors` and the Swift definitions; empty when
  /// there are none. A test expects none: `#expect(failures == [])`.
  ///
  /// (No `Testing` helper here: a module that imports `Testing` links
  /// `Testing.framework`, which only test bundles find on Apple platforms.)
  ///
  /// - Parameters:
  ///   - vectors: The contents of the `drift-vectors.json` that
  ///     `zero-swift drift record` wrote.
  ///   - schema: The app's schema, which the mutator samples' rows are in.
  ///   - queries: Every named query of the app.
  ///   - mutators: Every mutator of the app.
  public static func failures<Context: Decodable & Sendable>(
    vectors: String, schema: ZeroSchema, queries: [any NamedQuery<Context>],
    mutators: [any Mutator<Context>]
  ) throws -> [String] {
    let v = try JSONParser.parse(vectors)
    var failures: [String] = []

    let tsQueries = Set(v["queries"]?.arrayValue?.compactMap(\.stringValue) ?? [])
    let tsMutators = Set(v["mutators"]?.arrayValue?.compactMap(\.stringValue) ?? [])
    let swiftQueries = Dictionary(queries.map { ($0.name, $0) }) { a, _ in a }
    let swiftMutators = Dictionary(mutators.map { ($0.name, $0) }) { a, _ in a }
    for name in tsQueries.subtracting(swiftQueries.keys).sorted() {
      failures.append("query \(name): no Swift definition")
    }
    for name in Set(swiftQueries.keys).subtracting(tsQueries).sorted() {
      failures.append("query \(name): not in the TS registry")
    }
    for name in tsMutators.subtracting(swiftMutators.keys).sorted() {
      failures.append("mutator \(name): no Swift definition")
    }
    for name in Set(swiftMutators.keys).subtracting(tsMutators).sorted() {
      failures.append("mutator \(name): not in the TS registry")
    }

    for (i, sample) in (v["querySamples"]?.arrayValue ?? []).enumerated() {
      let name = sample["name"]?.stringValue ?? ""
      let label = "query \(name) sample \(i)"
      guard let definition = swiftQueries[name] else { continue }
      do {
        failures += try checkQuery(label, sample, definition)
      } catch {
        failures.append("\(label): \(error)")
      }
    }
    for (i, sample) in (v["mutatorSamples"]?.arrayValue ?? []).enumerated() {
      let name = sample["name"]?.stringValue ?? ""
      let label = "mutator \(name) sample \(i)"
      guard let mutator = swiftMutators[name] else { continue }
      do {
        failures += try checkMutator(label, sample, mutator, schema: schema, all: mutators)
      } catch {
        failures.append("\(label): \(error)")
      }
    }
    return failures
  }

  private static func context<Context: Decodable>(_ sample: JSONValue) throws -> Context {
    try JSONValueDecoder.decode(Context.self, from: sample["context"] ?? .null)
  }

  private static func checkQuery<Context: Decodable & Sendable>(
    _ label: String, _ sample: JSONValue, _ definition: any NamedQuery<Context>
  ) throws -> [String] {
    let args = sample["args"]?.arrayValue ?? []
    let (query, sent) = try definition.query(json: args, context: context(sample))
    var failures: [String] = []
    if sent != args {
      failures.append(
        "\(label): the arguments encode as \(JSONSerializer.stringify(.array(sent))), "
          + "not \(JSONSerializer.stringify(.array(args)))")
    }
    let hash = QueryHash.hashOfAST(query.ast)
    if let expected = sample["hash"]?.stringValue, hash != expected {
      failures.append(
        "\(label): the AST differs.\n  TS:    "
          + JSONSerializer.stringify(sample["ast"] ?? .null) + "\n  Swift: "
          + JSONSerializer.stringify(query.ast.json))
    }
    return failures
  }

  private static func checkMutator<Context: Decodable & Sendable>(
    _ label: String, _ sample: JSONValue, _ mutator: any Mutator<Context>, schema: ZeroSchema,
    all: [any Mutator<Context>]
  ) throws -> [String] {
    var failures: [String] = []
    let args = sample["args"]
    let sent = try mutator.reencode(args: args)
    if sent != args {
      failures.append(
        "\(label): the arguments encode as \(sent.map(JSONSerializer.stringify) ?? "undefined"), "
          + "not \(args.map(JSONSerializer.stringify) ?? "undefined")")
    }

    var options = ZeroOptions(userID: "drift", storageKey: randomStorageKey(), storage: .memory)
    options.logSink = RecordingLogSink()
    let zero = try Zero(
      schema: schema, context: try context(sample) as Context,
      mutators: all + [SeedMutator<Context>()], options: options,
      scheduler: ManualScheduler(now: 0), socketFactory: nil)
    defer { zero.close() }
    zero.mutate(MutateRequest(mutator: SeedMutator<Context>(), args: sample["seed"] ?? [:]))
    let before = try rows(zero, schema)
    let result = zero.mutate(MutateRequest(mutator: mutator, args: args))
    let after = try rows(zero, schema)

    let outcome = result.client.result.map { r in (try? r.get())?.json ?? .null } ?? .null
    if let expected = sample["outcome"], outcome != expected {
      failures.append(
        "\(label): the outcome is \(JSONSerializer.stringify(outcome)), not "
          + JSONSerializer.stringify(expected))
    }
    let changes = diff(before, after, schema)
    let expected = sample["changes"] ?? []
    if changes != expected {
      failures.append(
        "\(label): the changes differ.\n  TS:    \(JSONSerializer.stringify(expected))\n  Swift: "
          + JSONSerializer.stringify(changes))
    }
    return failures
  }

  /// Rows by table, then by primary key.
  private typealias Tables = [String: [String: Row]]

  /// Every row of every table.
  private static func rows<Context>(_ zero: Zero<Context>, _ schema: ZeroSchema) throws -> Tables {
    var out: Tables = [:]
    for table in schema.tableNames {
      let pk = schema.tables[table]?.primaryKey ?? []
      let data = try zero.run(AnyQuery(schema, table)).result?.get() ?? .many([])
      var byKey: [String: Row] = [:]
      for entry in data.entries {
        byKey[JSONSerializer.stringify(.array(pk.map { entry.row[$0] ?? .null }))] = entry.row
      }
      out[table] = byKey
    }
    return out
  }

  /// The changes from `before` to `after`, as `zero-swift drift record`
  /// lists them: by table in schema order, then by primary key.
  private static func diff(
    _ before: Tables, _ after: Tables, _ schema: ZeroSchema
  ) -> JSONValue {
    var changes: [JSONValue] = []
    for table in schema.tableNames {
      let pk = schema.tables[table]?.primaryKey ?? []
      let old = before[table] ?? [:]
      let new = after[table] ?? [:]
      for key in Set(old.keys).union(new.keys).sorted(by: { StringCompare.utf8($0, $1) < 0 }) {
        switch (old[key], new[key]) {
        case (let o, let n?) where o != n:
          changes.append(["table": .string(table), "op": "put", "row": JSONValue(n)])
        case (let o?, nil):
          var id: JSONObject = [:]
          for c in pk { id[c] = o[c] ?? .null }
          changes.append(["table": .string(table), "op": "del", "id": .object(id)])
        default:
          break
        }
      }
    }
    return .array(changes)
  }

  private static func randomStorageKey() -> String {
    "drift-" + String(UInt64.random(in: 0...UInt64.max), radix: 36)
  }
}

/// Puts the sample's rows (`{table: [row]}`) before the mutator runs.
private struct SeedMutator<Context: Sendable>: Mutator {
  let name = "__drift.seed"

  @MainActor
  func run(_ tx: Transaction, args: JSONValue?, context: Context) throws -> JSONValue? {
    for (table, rows) in args?.objectValue ?? [:] {
      for row in rows.arrayValue ?? [] {
        try tx.mutate[dynamicMember: table].upsert(Row(row.objectValue ?? [:]))
      }
    }
    return nil
  }
}
