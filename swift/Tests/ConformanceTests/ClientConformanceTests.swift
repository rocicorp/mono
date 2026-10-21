import Foundation
import Testing
import ZeroTesting

@testable import Zero

/// Replays the client scenarios (`packages/zero-conformance/src/client/`)
/// and compares what the app and the server observe after every step with
/// what the JS client did. The runner mirrors `harness.ts`.
@Suite(.enabled(if: Vectors.available))
@MainActor
struct ClientConformanceTests {
  @Test func scenariosOnMemory() throws {
    try Self.run(storage: .memory)
  }

  @Test func scenariosOnSQLite() throws {
    let directory = NSTemporaryDirectory() + "zero-client-scenarios-" + UUID().uuidString
    defer { try? FileManager.default.removeItem(atPath: directory) }
    try Self.run(storage: .sqlite(directory: directory))
  }

  static func run(storage: ZeroStorage) throws {
    let file = try Vectors.file("client")
    let startTime = file["shared"]?["startTime"]?.numberValue ?? 0
    let only = ProcessInfo.processInfo.environment["ZERO_CLIENT_SCENARIO"]
    var failed = 0
    var passed = 0
    for scenario in file["cases"]?.arrayValue ?? [] {
      let name = scenario["name"]?.stringValue ?? "?"
      if let only, !name.contains(only) { continue }
      let expected = scenario["expect"]?.arrayValue ?? []
      let runner = try ScenarioRunner(scenario, startTime: startTime, storage: storage)
      let actual = runner.run()
      if let mismatch = Self.firstMismatch(scenario, expected, actual) {
        failed += 1
        Issue.record(Comment(rawValue: "\(name)\n\(mismatch)"))
      } else {
        passed += 1
      }
    }
    print("[client scenarios on \(storage)] \(passed) passed, \(failed) failed")
  }

  static func firstMismatch(
    _ scenario: JSONValue, _ expected: [JSONValue], _ actual: [JSONValue]
  )
    -> String?
  {
    let steps = scenario["steps"]?.arrayValue ?? []
    for i in 0..<max(expected.count, actual.count) {
      let e = i < expected.count ? expected[i] : .null
      let a = i < actual.count ? actual[i] : .null
      if e != a {
        let step = i < steps.count ? JSONSerializer.stringify(steps[i]) : "?"
        var lines = ["step \(i): \(step)"]
        let keys = Set((e.objectValue ?? [:]).keys).union((a.objectValue ?? [:]).keys).sorted()
        for key in keys where e[key] != a[key] {
          lines.append("  \(key):")
          lines.append("    expected \(e[key].map(JSONSerializer.stringify) ?? "(none)")")
          lines.append("    actual   \(a[key].map(JSONSerializer.stringify) ?? "(none)")")
        }
        return lines.joined(separator: "\n")
      }
    }
    return nil
  }
}

/// Runs one scenario (`runScenario` in `harness.ts`).
@MainActor
final class ScenarioRunner {
  let scenario: JSONValue
  let scheduler: ManualScheduler
  let factory = MockSocketFactory()
  var zero: Zero<Void>!
  var events: [JSONValue] = []
  var placeholders: [String: String] = [:]

  final class ViewEntryState {
    let view: MaterializedView
    var data: JSONValue = .null
    var resultType = ""
    var notifications = 0
    init(_ view: MaterializedView) { self.view = view }
  }

  final class Settled {
    var value: JSONValue = "pending"
  }

  var views: [(String, ViewEntryState)] = []
  var retired: [ViewEntryState] = []
  var mutations: [(String, Settled, Settled)] = []
  var runs: [(String, Settled)] = []
  var preloads: [(String, Settled, Settled, () -> Void)] = []

  let storage: ZeroStorage
  /// The scenario's own storage, as `#scenario-<n>` in `harness.ts`.
  let scenarioKey: Int
  static var scenarioCount = 0
  var restarts = 0
  /// The IDs placeholders in downstream messages stand for.
  var ids: [String: String] = [:]

  init(_ scenario: JSONValue, startTime: Double, storage: ZeroStorage) throws {
    self.scenario = scenario
    self.storage = storage
    Self.scenarioCount += 1
    scenarioKey = Self.scenarioCount
    scheduler = ManualScheduler(now: startTime)
    zero = try makeZero(scenario["options"] ?? [:])
    placeholders = [zero.clientID: "$clientID", zero.clientGroupID: "$clientGroupID"]
    ids = ["$clientID": zero.clientID]
  }

  func makeZero(_ o: JSONValue, mutators registered: JSONValue? = nil) throws -> Zero<Void> {
    var options = ZeroOptions()
    options.logSink = RecordingLogSink()
    switch o["cacheURL"] {
    case .null?: options.cacheURL = nil
    case .string(let s)?: options.cacheURL = s
    default: options.cacheURL = "http://zero.test"
    }
    switch o["userID"] {
    case .null?: options.userID = nil
    case .string(let s)?: options.userID = s
    default: options.userID = "u1"
    }
    options.auth = o["auth"]?.stringValue
    if let v = o["pingTimeoutMs"]?.numberValue { options.pingTimeoutMs = v }
    if let v = o["disconnectTimeoutMs"]?.numberValue { options.disconnectTimeoutMs = v }
    if let v = o["queryChangeThrottleMs"]?.numberValue { options.queryChangeThrottleMs = v }
    if let v = o["maxHeaderLength"]?.numberValue { options.maxHeaderLength = Int(v) }
    options.hiddenDisconnectDelayMs = o["hiddenTabDisconnectDelay"]?.numberValue ?? 300_000
    options.mutateURL = o["mutateURL"]?.stringValue
    options.mutateHeaders = o["mutateHeaders"]?.objectValue?.compactMapValues(\.stringValue)
    options.queryURL = o["queryURL"]?.stringValue
    options.queryHeaders = o["queryHeaders"]?.objectValue?.compactMapValues(\.stringValue)
    options.storageKey = "\(o["storageKey"]?.stringValue ?? "")#scenario-\(scenarioKey)"
    options.storage = storage
    options.onUpdateNeeded = { [weak self] reason in
      self?.events.append(["onUpdateNeeded": reason.json])
    }
    options.onClientStateNotFound = { [weak self] in
      self?.events.append("onClientStateNotFound")
    }
    let mutators =
      registered?.stringValue == "issues"
      ? ClientApp.mutators.filter { $0.name.hasPrefix("issue.") } : ClientApp.mutators
    return try Zero(
      schema: ClientApp.schema, mutators: mutators, options: options, scheduler: scheduler,
      socketFactory: factory)
  }

  /// Closes the client and starts a new one on the same storage
  /// (`restart` in `harness.ts`).
  func restart(_ step: JSONValue) throws {
    zero.close()
    settle()
    if let ms = step["advance"]?.numberValue { scheduler.advance(by: ms) }
    let n = restarts
    restarts += 1
    let old = (clientID: zero.clientID, clientGroupID: zero.clientGroupID)
    zero = try makeZero(step["options"] ?? scenario["options"] ?? [:], mutators: step["mutators"])
    placeholders[old.clientID] = "$clientID.\(n)"
    ids["$clientID.\(n)"] = old.clientID
    if old.clientGroupID != zero.clientGroupID {
      placeholders[old.clientGroupID] = "$clientGroupID.\(n)"
    }
    placeholders[zero.clientID] = "$clientID"
    placeholders[zero.clientGroupID] = "$clientGroupID"
    ids["$clientID"] = zero.clientID
    views = []
    retired = []
    mutations = []
    runs = []
    preloads = []
  }

  func normalize(_ v: JSONValue, key: String? = nil) -> JSONValue {
    switch v {
    case .string(let s):
      if key == "requestID" { return "$requestID" }
      return .string(placeholders[s] ?? s)
    case .array(let a):
      return .array(a.map { normalize($0) })
    case .object(let o):
      var out: [String: JSONValue] = [:]
      for (k, e) in o { out[placeholders[k] ?? k] = normalize(e, key: k) }
      return .object(out)
    default:
      return v
    }
  }

  func substitute(_ v: JSONValue) -> JSONValue {
    switch v {
    case .string(let s):
      return .string(ids[s] ?? s)
    case .array(let a):
      return .array(a.map(substitute))
    case .object(let o):
      var out: [String: JSONValue] = [:]
      for (k, e) in o { out[ids[k] ?? k] = substitute(e) }
      return .object(out)
    default:
      return v
    }
  }

  func socket(_ index: JSONValue?) throws -> MockSocket {
    let sockets = factory.sockets
    if let i = index?.numberValue {
      guard Int(i) < sockets.count else { throw ClientApp.Failure("No socket") }
      return sockets[Int(i)]
    }
    guard let last = sockets.last else { throw ClientApp.Failure("No socket") }
    return last
  }

  static func ttl(_ v: JSONValue?) -> TTL? {
    switch v {
    case .number(let n)?: .milliseconds(n)
    case .string(let s)?: .string(s)
    default: nil
    }
  }

  func perform(_ step: JSONValue) throws {
    let op = step["op"]?.stringValue ?? ""
    switch op {
    case "materialize":
      let query = try ClientApp.request(step["query"]!.stringValue!, step["args"])
      let view = try zero.materialize(query, ttl: Self.ttl(step["ttl"]) ?? .defaultTTL)
      let entry = ViewEntryState(view)
      let name = step["view"]!.stringValue!
      // A view replaced under its name stays alive, as in JS.
      retired += views.filter { $0.0 == name }.map(\.1)
      views.removeAll { $0.0 == name }
      views.append((name, entry))
      _ = view.addListener { data, resultType, _ in
        entry.data = data.json ?? .null
        entry.resultType = resultType.rawValue
        entry.notifications += 1
      }
    case "destroy":
      let name = step["view"]!.stringValue!
      views.first { $0.0 == name }?.1.view.destroy()
      views.removeAll { $0.0 == name }
    case "updateTTL":
      let name = step["view"]!.stringValue!
      if let ttl = Self.ttl(step["ttl"]) { views.first { $0.0 == name }?.1.view.updateTTL(ttl) }
    case "preload":
      let query = try ClientApp.request(step["query"]!.stringValue!, step["args"])
      let p = zero.preload(query, ttl: Self.ttl(step["ttl"]) ?? .defaultPreloadTTL)
      let cached = Settled()
      let complete = Settled()
      preloads.append((step["preload"]!.stringValue!, cached, complete, p.cleanup))
      p.cached.onSettle { cached.value = Self.settled($0) }
      p.complete.onSettle { complete.value = Self.settled($0) }
    case "cleanup":
      let name = step["preload"]!.stringValue!
      if let p = preloads.first(where: { $0.0 == name }) {
        zero.engine.loop.dispatch { p.3() }
      }
    case "run":
      let name = step["run"]!.stringValue!
      let entry = Settled()
      runs.removeAll { $0.0 == name }
      runs.append((name, entry))
      let query = try ClientApp.request(step["query"]!.stringValue!, step["args"])
      let type = RunType(rawValue: step["type"]?.stringValue ?? "unknown")!
      let result = try zero.run(query, type: type)
      result.onSettle { r in
        switch r {
        case .success(let data): entry.value = ["data": data.json ?? .null]
        case .failure(let e as QueryError): entry.value = ["rejected": e.data?.json ?? .null]
        case .failure: entry.value = ["rejected": [:]]
        }
      }
    case "mutate":
      let name = step["mutator"]!.stringValue!
      guard ClientApp.mutators.contains(where: { $0.name == name }) else {
        throw ClientApp.Failure("Unknown name \(name)")
      }
      let result = zero.engine.loop.dispatch {
        zero.engine.mutate(name: name, args: step["args"])
      }
      let client = Settled()
      let server = Settled()
      mutations.append((step["mutation"]!.stringValue!, client, server))
      result.client.onSettle { if case .success(let o) = $0 { client.value = o.json } }
      result.server.onSettle { if case .success(let o) = $0 { server.value = o.json } }
    case "receive":
      try socket(step["socket"]).receive(
        text: JSONSerializer.stringify(substitute(step["message"]!)))
    case "receiveAll":
      let s = try socket(step["socket"])
      for m in step["messages"]?.arrayValue ?? [] {
        s.receive(text: JSONSerializer.stringify(substitute(m)))
      }
    case "receiveChunk":
      let payload: [UInt8]
      if let bytes = step["bytes"]?.arrayValue {
        payload = bytes.map { UInt8($0.numberValue!) }
      } else if let text = step["text"]?.stringValue {
        payload = Array(text.utf8)
      } else {
        payload = Array(JSONSerializer.stringify(substitute(step["parts"] ?? [])).utf8)
      }
      try socket(step["socket"]).receive(.binary([PokeChunk.messageType] + payload))
    case "socketClose":
      try socket(step["socket"]).serverClose(
        code: Int(step["code"]?.numberValue ?? 1006), reason: step["reason"]?.stringValue ?? "",
        wasClean: step["wasClean"]?.boolValue ?? false)
    case "advance":
      scheduler.advance(by: step["ms"]?.numberValue ?? 0)
    case "visibility":
      zero.setVisible(step["state"] == "visible")
    case "connect":
      zero.connection.connect(auth: step["auth"]?.stringValue)
    case "close":
      zero.close()
    case "restart":
      try restart(step)
    default:
      throw ClientApp.Failure("Unknown op \(op)")
    }
  }

  static func settled<T>(_ r: Result<T, any Error>) -> JSONValue {
    switch r {
    case .success: "resolved"
    case .failure(let e as QueryError): ["rejected": e.error?.json ?? .null]
    case .failure: ["rejected": [:]]
    }
  }

  /// Settles the work due now (`settle` in `harness.ts`).
  func settle() {
    zero.engine.loop.dispatch {}
    scheduler.runDueTimers()
  }

  func run() -> [JSONValue] {
    var results: [JSONValue] = []
    for step in scenario["steps"]?.arrayValue ?? [] {
      let sockets = factory.sockets
      let sentCounts = sockets.map(\.messages.count)
      let closedBefore = sockets.map(\.closed)
      var result: [String: JSONValue] = [:]
      do {
        try perform(step)
      } catch {
        result["threw"] = .string(errorMessage(error))
      }
      settle()

      let now = factory.sockets
      let opened = now[sockets.count...].map { s -> JSONValue in
        [
          "url": socketURL(s.url),
          "protocol": normalize((try? SecProtocols.decode(s.secProtocol ?? "")) ?? .null),
        ]
      }
      if !opened.isEmpty { result["opened"] = .array(opened) }
      var sent: [JSONValue] = []
      for (i, s) in now.enumerated() {
        let from = i < sentCounts.count ? sentCounts[i] : 0
        for m in s.messages[from...] {
          sent.append(normalize((try? JSONParser.parse(Array(m.utf8))) ?? .string(m)))
        }
      }
      if !sent.isEmpty { result["sent"] = .array(sent) }
      var closed: [JSONValue] = []
      for (i, s) in now.enumerated() where s.closed && !(i < closedBefore.count && closedBefore[i])
      {
        closed.append(.number(Double(i)))
      }
      if !closed.isEmpty { result["closed"] = .array(closed) }
      result["state"] = normalize(zero.connection.state.json)
      if !views.isEmpty {
        var o: [String: JSONValue] = [:]
        for (name, v) in views {
          o[name] = [
            "data": v.data, "resultType": .string(v.resultType),
            "notifications": .number(Double(v.notifications)),
          ]
          v.notifications = 0
        }
        result["views"] = .object(o)
      }
      if !mutations.isEmpty {
        var o: [String: JSONValue] = [:]
        for (name, c, s) in mutations { o[name] = ["client": c.value, "server": s.value] }
        result["mutations"] = .object(o)
      }
      if !runs.isEmpty {
        var o: [String: JSONValue] = [:]
        for (name, r) in runs { o[name] = r.value }
        result["runs"] = .object(o)
      }
      if !preloads.isEmpty {
        var o: [String: JSONValue] = [:]
        for (name, c, p, _) in preloads { o[name] = ["cached": c.value, "complete": p.value] }
        result["preloads"] = .object(o)
      }
      if !events.isEmpty {
        result["events"] = .array(events)
        events = []
      }
      results.append(.object(result))
    }
    zero.close()
    return results
  }

  /// The connect URL with its query parameters in order (`socketURL`).
  func socketURL(_ url: String) -> JSONValue {
    let (base, query) =
      url.firstIndex(of: "?").map { (String(url[..<$0]), String(url[url.index(after: $0)...])) }
      ?? (url, "")
    let schemeEnd = base.range(of: "://")!.upperBound
    let pathStart = base[schemeEnd...].firstIndex(of: "/") ?? base.endIndex
    var pairs: [JSONValue] = []
    for part in query.split(separator: "&") {
      let kv = part.split(separator: "=", maxSplits: 1, omittingEmptySubsequences: false)
      let k = Self.formDecode(String(kv[0]))
      let v = kv.count > 1 ? Self.formDecode(String(kv[1])) : ""
      switch k {
      case "ts": continue
      case "wsid", "profileID": pairs.append([.string(k), .string("$" + k)])
      default: pairs.append([.string(k), .string(placeholders[v] ?? v)])
      }
    }
    return [
      "origin": .string(String(base[..<pathStart])), "path": .string(String(base[pathStart...])),
      "query": .array(pairs),
    ]
  }

  static func formDecode(_ s: String) -> String {
    var bytes: [UInt8] = []
    var it = Array(s.utf8)[...]
    while let b = it.popFirst() {
      if b == UInt8(ascii: "+") {
        bytes.append(UInt8(ascii: " "))
      } else if b == UInt8(ascii: "%"), it.count >= 2,
        let v = UInt8(String(decoding: it.prefix(2), as: UTF8.self), radix: 16)
      {
        bytes.append(v)
        it = it.dropFirst(2)
      } else {
        bytes.append(b)
      }
    }
    return String(decoding: bytes, as: UTF8.self)
  }
}
