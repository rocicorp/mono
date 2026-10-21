import Zero

/// A server in the app's own process, for previews, demos, and UI tests of
/// screens that need synced data without a zero-cache: it holds rows, sends
/// them all when a client connects, and confirms every query the client
/// wants, so its views complete. `put(_:_:)` syncs a row to every client,
/// as another user's change would.
///
/// It doesn't run queries or mutators: the client answers queries from the
/// rows it has, and its own mutations stay pending. Pass it as `Zero`'s `socketFactory`, with any `cacheURL`.
///
/// ```swift
/// let server = LoopbackServer()
/// server.put("message", ["id": "m1", "body": "Hi", "createdAt": 1])
/// let zero = try Zero(schema: .chat, options: options, socketFactory: server)
/// ```
@MainActor
public final class LoopbackServer: SocketFactory {
  private var rows: [RowKey: JSONValue] = [:]
  private var order: [RowKey] = []
  private var sockets: [LoopbackSocket] = []

  public init() {}

  /// Adds `row` to `table`, or replaces the row with the same primary key
  /// (`primaryKey` names its columns), and sends it to connected clients.
  public func put(_ table: String, _ row: [String: JSONValue], primaryKey: [String] = ["id"]) {
    let key = RowKey(table: table, id: primaryKey.map { row[$0] ?? .null })
    if rows.updateValue(.object(row), forKey: key) == nil { order.append(key) }
    let op = Self.put(table, .object(row))
    for socket in sockets where socket.connected { socket.poke(rows: [op]) }
  }

  /// `put` for each row, in one sync.
  public func put(_ table: String, _ rows: [[String: JSONValue]], primaryKey: [String] = ["id"]) {
    for row in rows {
      let key = RowKey(table: table, id: primaryKey.map { row[$0] ?? .null })
      if self.rows.updateValue(.object(row), forKey: key) == nil { order.append(key) }
    }
    let ops = rows.map { Self.put(table, .object($0)) }
    for socket in sockets where socket.connected { socket.poke(rows: ops) }
  }

  public func open(
    url: String, secProtocol: String?, events: any SocketEvents
  )
    -> any SocketConnection
  {
    sockets.removeAll { $0.closed }
    let socket = LoopbackSocket(secProtocol: secProtocol, events: events)
    sockets.append(socket)
    socket.later {
      socket.events?.socketDidOpen()
      socket.deliver(#"["connected",{"wsid":"loopback"}]"#)
      socket.connected = true
      // Everything so far, and the queries asked for before connecting.
      socket.poke(
        rows: self.order.compactMap { k in self.rows[k].map { Self.put(k.table, $0) } },
        queries: socket.got.sorted())
    }
    return socket
  }

  private static func put(_ table: String, _ value: JSONValue) -> JSONValue {
    .object(["op": .string("put"), "tableName": .string(table), "value": value])
  }
}

private struct RowKey: Hashable {
  var table: String
  var id: [JSONValue]
}

@MainActor
private final class LoopbackSocket: SocketConnection {
  weak var events: (any SocketEvents)?
  var connected = false
  private(set) var closed = false
  private(set) var got: Set<String> = []
  private var cookie = 0

  init(secProtocol: String?, events: any SocketEvents) {
    self.events = events
    // The first queries can ride in the connection's protocol header.
    if let secProtocol, let decoded = try? SecProtocols.decode(secProtocol) {
      later { self.receiveFromClient(decoded) }
    }
  }

  /// Runs `action` on a later turn, as a network would deliver it.
  func later(_ action: @escaping @MainActor () -> Void) {
    Task { @MainActor [weak self] in
      guard let self, !self.closed else { return }
      action()
    }
  }

  func deliver(_ text: String) { events?.socketDidReceive(.text(text)) }

  func send(_ text: String) {
    guard let json = try? JSONParser.parse(text) else { return }
    later { self.receiveFromClient(json) }
  }

  func close(code: Int?) { closed = true }

  private func receiveFromClient(_ json: JSONValue) {
    if case .array(let message) = json, message.first == .string("ping") {
      deliver(#"["pong",{}]"#)
      return
    }
    var wanted: [String] = []
    collect(json, &wanted)
    let new = wanted.filter { !got.contains($0) }
    got.formUnion(new)
    // Before connecting, the connect poke confirms them.
    if connected, !new.isEmpty { poke(queries: new) }
  }

  func poke(rows: [JSONValue] = [], queries: [String] = []) {
    let base: JSONValue = cookie == 0 ? .null : .string(Self.cookie(cookie))
    cookie += 1
    let id = JSONValue.string("p\(cookie)")
    var part: [String: JSONValue] = ["pokeID": id]
    if !rows.isEmpty { part["rowsPatch"] = .array(rows) }
    if !queries.isEmpty {
      part["gotQueriesPatch"] = .array(
        queries.map { .object(["op": .string("put"), "hash": .string($0)]) })
    }
    deliver(.string("pokeStart"), .object(["pokeID": id, "baseCookie": base]))
    deliver(.string("pokePart"), .object(part))
    deliver(.string("pokeEnd"), .object(["pokeID": id, "cookie": .string(Self.cookie(cookie))]))
  }

  private func deliver(_ tag: JSONValue, _ body: JSONValue) {
    deliver(JSONSerializer.stringify(.array([tag, body])))
  }

  private static func cookie(_ n: Int) -> String {
    let s = String(n)
    return String(repeating: "0", count: max(0, 6 - s.count)) + s
  }

  /// The hashes of the queries `json` puts in the desired set.
  private func collect(_ json: JSONValue, _ out: inout [String]) {
    switch json {
    case .array(let a): for v in a { collect(v, &out) }
    case .object(let o):
      if o["op"] == .string("put"), case .string(let hash)? = o["hash"] { out.append(hash) }
      for v in o.values { collect(v, &out) }
    default: break
    }
  }
}
