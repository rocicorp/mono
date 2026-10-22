import Foundation

/// Times benchmarks the way `packages/shared/src/bench.ts` does: many
/// samples over a time budget, reported as the median time per call with
/// the 75th and 99th percentiles.
@MainActor
final class Bench {
  struct Result: Encodable {
    var name: String
    /// Nanoseconds per call.
    var median: Double?
    var p75: Double?
    var p99: Double?
    var min: Double?
    var samples: Int?
    /// Memory benchmarks: bytes.
    var bytes: Int?
  }

  let budget: Double
  let filters: [String]
  private(set) var results: [Result] = []
  private var group = ""

  init(budget: Double, filters: [String]) {
    self.budget = budget
    self.filters = filters
  }

  /// Names the benchmarks `body` defines `"<name> > …"`, and skips them
  /// (and their setup) unless a filter's part before ` > ` matches `name`.
  func group(_ name: String, _ body: () throws -> Void) rethrows {
    let wanted =
      filters.isEmpty
      || filters.contains { filter in
        let part = filter.components(separatedBy: " > ")[0]
        return name.localizedCaseInsensitiveContains(part)
      }
    guard wanted else { return }
    group = name
    defer { group = "" }
    print("\n\(name)")
    try body()
  }

  /// Whether the group's benchmark `name` runs, for skipping its setup.
  func wants(_ name: String) -> Bool { selected("\(group) > \(name)") }

  private func selected(_ name: String) -> Bool {
    filters.isEmpty || filters.contains { name.localizedCaseInsensitiveContains($0) }
  }

  /// Times `body`, called back to back in batches long enough for the
  /// clock to resolve.
  func run(_ name: String, _ body: () throws -> Void) rethrows {
    let full = "\(group) > \(name)"
    guard selected(full) else { return }
    var batch = 1
    while batch < 1 << 20 {
      let t = try time { for _ in 0..<batch { try body() } }
      if t >= 50_000 { break }
      batch *= 2
    }
    try record(full) { () throws -> Double in
      try time { for _ in 0..<batch { try body() } } / Double(batch)
    }
  }

  /// Times `body` with what `setup` returns, made fresh for each call and
  /// not timed.
  func run<State>(
    _ name: String, setup: () throws -> State, _ body: (inout State) throws -> Void
  ) rethrows {
    let full = "\(group) > \(name)"
    guard selected(full) else { return }
    try record(full) { () throws -> Double in
      var state = try setup()
      return try time { try body(&state) }
    }
  }

  private func record(_ name: String, _ sample: () throws -> Double) rethrows {
    // Warm up for a tenth of the budget.
    let start = Self.now()
    while Self.now() - start < budget * 1e8 { _ = try sample() }
    var samples: [Double] = []
    let measure = Self.now()
    while samples.count < 10 || (Self.now() - measure < budget * 1e9 && samples.count < 1_000_000) {
      samples.append(try sample())
    }
    samples.sort()
    let (median, p75, p99) = (
      samples[samples.count / 2], samples[samples.count * 3 / 4], samples[samples.count * 99 / 100]
    )
    results.append(
      Result(
        name: name, median: median, p75: p75, p99: p99, min: samples[0], samples: samples.count))
    print(
      "  \(label(name)) \(Self.format(median).leftPad(10))/iter"
        + "  p75 \(Self.format(p75).leftPad(10))  p99 \(Self.format(p99).leftPad(10))"
        + "  min \(Self.format(samples[0]).leftPad(10))")
  }

  /// Records a measured amount of memory.
  func record(_ name: String, bytes: Int) {
    let full = "\(group) > \(name)"
    guard selected(full) else { return }
    results.append(Result(name: full, bytes: bytes))
    print("  \(label(full)) \(String(format: "%.1f MB", Double(bytes) / 1e6).leftPad(10))")
  }

  private func label(_ name: String) -> String {
    String(name.dropFirst(group.count + 3)).padding(toLength: 56, withPad: " ", startingAt: 0)
  }

  private func time(_ body: () throws -> Void) rethrows -> Double {
    let start = Self.now()
    try body()
    return Self.now() - start
  }

  /// Nanoseconds on the monotonic clock.
  static func now() -> Double {
    let d = ContinuousClock.now - epoch
    return Double(d.components.seconds) * 1e9 + Double(d.components.attoseconds) / 1e9
  }
  private static let epoch = ContinuousClock.now

  static func format(_ ns: Double) -> String {
    switch ns {
    case ..<1e3: String(format: "%.2f ns", ns)
    case ..<1e6: String(format: "%.2f µs", ns / 1e3)
    case ..<1e9: String(format: "%.2f ms", ns / 1e6)
    default: String(format: "%.2f s", ns / 1e9)
    }
  }
}

extension String {
  func leftPad(_ n: Int) -> String {
    count >= n ? self : String(repeating: " ", count: n - count) + self
  }
}

/// Keeps the optimizer from dropping the computation of `value`
/// (`do_not_optimize` in mitata).
@inline(never)
func use<T>(_ value: T) {
  withExtendedLifetime(value) {}
}

/// Checks a benchmark computed the right thing.
func check(_ condition: Bool, _ message: String) {
  if !condition {
    FileHandle.standardError.write(Data("check failed: \(message)\n".utf8))
    exit(1)
  }
}
