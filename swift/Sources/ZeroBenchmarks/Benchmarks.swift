import Foundation
import Zero

/// The client's benchmarks: ports of the JS benchmarks
/// (`zero-client/src/client/zero.bench.ts`, `shared/src/btree-set.bench.ts`,
/// `shared/src/hash.bench.ts`) under the same names, and the Swift client's
/// own (memory, hydration, pokes, first sync, and mutations).
///
///     swift run -c release ZeroBenchmarks [--time <seconds>] [--json <file>] [<filter>…]
///
/// A filter is a substring of `group > benchmark`, such as `hash` or
/// `pokes > put 100`.
@main
@MainActor
enum Benchmarks {
  static func main() throws {
    var filters: [String] = []
    var budget = 1.0
    var json: String?
    var args = CommandLine.arguments.dropFirst()
    while let arg = args.popFirst() {
      switch arg {
      case "--time": budget = args.popFirst().flatMap(Double.init) ?? budget
      case "--json": json = args.popFirst()
      default: filters.append(arg)
      }
    }
    let bench = Bench(budget: budget, filters: filters)

    // First, before the others allocate.
    try memory(bench)
    try zeroBench(bench)
    try hydration(bench)
    try pokes(bench)
    try firstSync(bench)
    try mutations(bench)
    btree(bench)
    hash(bench)

    if let json {
      let encoder = JSONEncoder()
      encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
      try encoder.encode(bench.results).write(to: URL(fileURLWithPath: json))
    }
  }

  /// What 100k rows cost in memory: the rows in the replica and the
  /// sources, and a view of them all.
  static func memory(_ bench: Bench) throws {
    try bench.group("memory") {
      let n = 100_000
      let before = residentMemory()
      let client = try Client(rows: n)
      let view = try client.zero.materialize(users)
      check(view.data.entries.count == n, "the view has every row")
      bench.record(
        "\(n) rows x 10 columns (numbers), with a view", bytes: residentMemory() - before)
      withExtendedLifetime(client) {}
    }
  }

  /// `zero.bench.ts`: views of 1000 rows of 10 number columns.
  static func zeroBench(_ bench: Bench) throws {
    try bench.group("zero.bench.ts") {
      let n = 1000
      let client = try Client(rows: n)
      let zero = client.zero
      try bench.run("All \(n) rows x 10 columns (numbers)") {
        let view = try zero.materialize(users)
        check(sum(view) == Double((n - 1) * n / 2), "every row")
        view.destroy()
      }
      try bench.run("pk = N") {
        let view = try zero.materialize(users.where(\.a, Double(n - 1)))
        check(sum(view) == Double(n - 1), "the last row")
        view.destroy()
      }
      try bench.run("Lower rows \(n / 2) x 10 columns (numbers)") {
        let view = try zero.materialize(users.where(\.a, .lt, Double(n / 2)))
        check(sum(view) == Double((n / 2 - 1) * n / 4), "the lower half")
        view.destroy()
      }
    }
  }

  /// Views of many rows, as the view's entries and as row structs.
  static func hydration(_ bench: Bench) throws {
    try bench.group("hydration") {
      for n in [1_000, 10_000, 100_000]
      where bench.wants("materialize \(n) rows") || bench.wants("materialize \(n) rows as structs")
      {
        let client = try Client(rows: n)
        let zero = client.zero
        try bench.run("materialize \(n) rows") {
          let view = try zero.materialize(users)
          check(view.data.entries.count == n, "every row")
          view.destroy()
        }
        try bench.run("materialize \(n) rows as structs") {
          let view = try zero.materialize(users)
          let rows = try [User](view: view.data)
          check(rows.count == n, "every row")
          view.destroy()
        }
      }
    }
  }

  /// Pokes from the server: parsing them, writing the replica, and
  /// updating a view of 1000 rows.
  static func pokes(_ bench: Bench) throws {
    try bench.group("pokes") {
      let n = 1000
      let client = try Client(rows: n, connected: true)
      let view = try client.zero.materialize(users)
      var value = n
      for k in [1, 100, 1000] {
        bench.run(
          "put \(k) changed rows, \(n)-row view",
          setup: { () -> [String] in
            value += 1
            return client.nextPoke(rows: (0..<k).map { User.json($0, value) })
          }
        ) { frames in
          client.receive(frames)
        }
        if value > n {
          check(view.data.entries[0].row["b"] == .number(Double(value)), "the poke applied")
        }
      }
    }
  }

  /// zbugs' first sync: one poke of 2,058 rows into a fresh client with
  /// the issue list's view, timed from the poke's frames arriving to the
  /// view being flushed. Its rows arrive by table in alphabetical order, so
  /// the issues come before the labels, the project, and the users that
  /// their joins and `exists` wait for.
  static func firstSync(_ bench: Bench) throws {
    try bench.group("first sync") {
      let rows = FirstSync.rows()
      let total = rows.count
      let perPart = (total + FirstSync.parts - 1) / FirstSync.parts
      let parts = stride(from: 0, to: total, by: perPart).map {
        Array(rows[$0..<min($0 + perPart, total)])
      }
      @MainActor func client() throws -> Client {
        try Client(rows: 0, connected: true, schema: FirstSync.schema, mutators: [])
      }
      try bench.run(
        "issueListV2 view, zbugs poke of \(total) rows in \(parts.count) parts",
        setup: { () throws -> (Client, MaterializedView, [String]) in
          let c = try client()
          let view = try c.zero.materialize(FirstSync.issueList)
          return (c, view, c.nextPoke(parts: parts))
        }
      ) { state in
        state.0.receive(state.2)
        let entries = state.1.data.entries
        check(entries.count == FirstSync.pageSize + 1, "a page of issues")
        check(entries.contains { $0.relationships["creator"] != .one(nil) }, "their creators")
      }
      // The same poke with no view: what the issue list adds is the
      // difference.
      try bench.run(
        "no view, zbugs poke of \(total) rows in \(parts.count) parts",
        setup: { () throws -> (Client, [String]) in
          let c = try client()
          return (c, c.nextPoke(parts: parts))
        }
      ) { state in
        state.0.receive(state.1)
      }
    }
  }

  /// Mutations, with a view of 1000 rows.
  static func mutations(_ bench: Bench) throws {
    try bench.group("mutations") {
      let n = 1000
      var value = n

      // Checks the row `value` last updated, if any.
      @MainActor func checkUpdated(_ view: MaterializedView, _ what: String) {
        guard value > n else { return }
        check(view.data.entries[value % n].row["b"] == .number(Double(value)), what)
      }

      let local = try Client(rows: n)
      let localView = try local.zero.materialize(users)
      bench.run("optimistic update, \(n)-row view") {
        value += 1
        local.zero.mutate(Mutators.updateUser(UpdateUser(a: value % n, value: value)))
      }
      checkUpdated(localView, "updated")

      // The same with the rows as structs, as an app reads them.
      let structs = try Client(rows: n)
      let live = try structs.zero.liveQuery(users)
      bench.run("optimistic update, \(n)-row live query of structs") {
        value += 1
        structs.zero.mutate(Mutators.updateUser(UpdateUser(a: value % n, value: value)))
      }
      check(value == n || live.data[value % n].b == Double(value), "read")

      // The client's side of a round trip: the optimistic update, the push,
      // and the poke that confirms it, which rebases the pending mutations.
      let remote = try Client(rows: n, connected: true)
      let remoteView = try remote.zero.materialize(users)
      var lastMutationID = 0
      bench.run("round trip, \(n)-row view") {
        value += 1
        lastMutationID += 1
        let a = value % n
        remote.zero.mutate(Mutators.updateUser(UpdateUser(a: a, value: value)))
        remote.scheduler.runDueTimers()
        remote.receive(
          remote.nextPoke(rows: [User.json(a, value)], lastMutationID: lastMutationID))
      }
      checkUpdated(remoteView, "confirmed")
    }
  }

  /// `btree-set.bench.ts`.
  static func btree(_ bench: Bench) {
    bench.group("btree-set.bench.ts") {
      let n = 1000
      let compare: (Int, Int) -> Int = { $0 - $1 }
      func make(_ size: Int) -> BTreeSet<Int> {
        var tree = BTreeSet<Int>(compare)
        for i in 0..<size { _ = tree.add(i) }
        return tree
      }
      let tree = make(n)
      let mid = n / 2
      bench.run("values() full scan") {
        var sum = 0
        var it = tree.values()
        while let v = it.next() { sum += v }
        use(sum)
      }
      bench.run("valuesFrom() from mid") {
        var sum = 0
        var it = tree.values(from: { $0 - mid })
        while let v = it.next() { sum += v }
        use(sum)
      }
      bench.run("valuesReversed() full scan") {
        var sum = 0
        var it = tree.valuesReversed(from: nil)
        while let v = it.next() { sum += v }
        use(sum)
      }
      bench.run("valuesFromReversed() from mid") {
        var sum = 0
        var it = tree.valuesReversed(from: { $0 - mid })
        while let v = it.next() { sum += v }
        use(sum)
      }
      bench.run("has() hit") {
        use(tree.contains(100))
        use(tree.contains(500))
        use(tree.contains(900))
      }
      bench.run("has() miss") {
        use(tree.contains(n + 1))
        use(tree.contains(n + 2))
        use(tree.contains(n + 3))
      }
      bench.run("get() hit") {
        use(tree.get(100))
        use(tree.get(500))
        use(tree.get(900))
      }
      var copy = make(n)
      bench.run("add() then delete() single key") {
        _ = copy.add(n + 1)
        _ = copy.delete(n + 1)
      }
      for size in [100, n] {
        bench.run("add() \(size) sequential keys") {
          use(make(size).count)
        }
      }
      let keys = Array(0..<n)
      bench.run("getOrCreateIndex pattern (old): add() loop after sort") {
        let reverse: (Int, Int) -> Int = { $1 - $0 }
        var t = BTreeSet<Int>(reverse)
        for k in keys.sorted(by: >) { _ = t.add(k) }
        use(t.count)
      }
    }
  }

  /// `hash.bench.ts`: the hashes of row keys and query ASTs.
  static func hash(_ bench: Bench) {
    func input(_ length: Int) -> String {
      var s = ""
      var i = 0
      while s.utf16.count < length {
        s += #"{"table":"issue\#(i)","column":"created\#(i)","value":\#(i)},"#
        i += 1
      }
      return String(s.prefix(length))
    }
    let sizes = [64, 256, 1024, 8192]
    for (name, f) in [
      ("h64", { (s: String) in use(XXHash32.h64(s)) }),
      ("h128", { (s: String) in use(XXHash32.h128(s)) }),
      ("h32", { (s: String) in use(XXHash32.h32(s)) }),
    ] {
      bench.group("hash.bench.ts > \(name)") {
        for size in sizes {
          let s = input(size)
          bench.run("\(size) chars") { f(s) }
        }
      }
    }
  }

  /// The sum of column `a` over the view's rows, as the JS benchmarks check.
  static func sum(_ view: MaterializedView) -> Double {
    sum(view.data.entries)
  }

  /// Reads `a` by index, as JS reads `row.a` off the object's shape, and off
  /// the main actor, which would check its executor for every row.
  nonisolated static func sum(_ entries: [ViewEntry]) -> Double {
    guard let first = entries.first else { return 0 }
    let a = first.row.layout.column("a")
    var total = 0.0
    for entry in entries { total += entry.row[column: a]?.numberValue ?? 0 }
    return total
  }
}
