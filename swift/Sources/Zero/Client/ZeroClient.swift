import Observation

/// A Zero client: syncs the rows of named queries from zero-cache into a
/// local replica, keeps materialized views of them current, and runs
/// mutators optimistically before the server confirms them
/// (`Zero` in `packages/zero-client/src/client/zero.ts`).
///
/// `Context` is what queries and mutators are given besides their
/// arguments, such as the logged-in user's auth data.
@MainActor
public final class Zero<Context: Sendable> {
  private(set) var engine: Engine
  public let context: Context
  public let connection: Connection

  private let schemaValue: ZeroSchema
  private let options: ZeroOptions
  private let functions: [String: MutatorFunction]
  private let scheduler: any Scheduler
  private let socketFactory: (any SocketFactory)?
  /// The views and preloads to move to a new client.
  private var views: [WeakView] = []
  private var preloads: [PreloadRecord] = []
  #if canImport(Darwin)
    private var lifecycle: AppLifecycle?
  #endif

  /// - Parameters:
  ///   - schema: The generated schema, such as `.app`.
  ///   - context: What queries and mutators are given besides their
  ///     arguments, such as the logged-in user's auth data.
  ///   - mutators: Every mutator the client may run. Pending mutations are
  ///     replayed by name, so each must be registered.
  ///   - options: Where to sync from, and how.
  ///   - scheduler: The clock and timers. With the system clock, the client
  ///     also follows the app's lifecycle and the network; tests pass a
  ///     manual one.
  ///   - socketFactory: Opens the sync socket. Defaults to
  ///     `URLSessionTransport` on Apple platforms.
  public init(
    schema: ZeroSchema, context: Context, mutators: [any Mutator<Context>] = [],
    options: ZeroOptions, scheduler: any Scheduler = SystemScheduler(),
    socketFactory: (any SocketFactory)? = platformSocketFactory()
  ) throws {
    var functions: [String: MutatorFunction] = [:]
    for m in mutators {
      guard functions[m.name] == nil else {
        throw ApplicationError("Mutator \"\(m.name)\" is registered twice")
      }
      functions[m.name] = { tx, args in try m.run(tx, args: args, context: context) }
    }
    engine = try Engine(
      schema: schema, options: options, mutators: functions, scheduler: scheduler,
      socketFactory: socketFactory)
    self.context = context
    self.schemaValue = schema
    self.options = options
    self.functions = functions
    self.scheduler = scheduler
    self.socketFactory = socketFactory
    connection = Connection(engine)
    attach(engine)
    #if canImport(Darwin)
      // Simulated time drives everything in tests, the app's lifecycle too.
      if scheduler is SystemScheduler { lifecycle = AppLifecycle(self) }
    #endif
  }

  public var clientID: String { engine.clientID }
  public var clientGroupID: String { engine.clientGroupID }
  public var userID: String? { engine.userID }
  public var schema: ZeroSchema { engine.schema }
  public var closed: Bool { engine.closed }

  private func attach(_ engine: Engine) {
    if options.onClientStateNotFound == nil {
      engine.onClientStateNotFound = { [weak self] in self?.replaceClient() }
    }
    engine.onUpdateNeeded = { [weak self] reason in self?.connection.updateNeeded = reason }
  }

  /// Replaces the client, which can't sync any longer, by a new one on the
  /// same store, and moves the views and preloads to it (the
  /// `onClientStateNotFound` default; the React provider does the same).
  func replaceClient() {
    let old = engine
    guard !old.closed else { return }
    let new: Engine
    do {
      new = try Engine(
        schema: schemaValue, options: options, mutators: functions, scheduler: scheduler,
        socketFactory: socketFactory, visible: old.visible)
    } catch {
      old.log.error("Failed to replace the client: \(error)")
      return
    }
    engine = new
    attach(new)
    connection.bind(new)
    views.removeAll { $0.view?.isDestroyed ?? true }
    for case let view? in views.map(\.view) {
      guard let query = view.query,
        let fresh = try? new.loop.call({ try new.context.materialize(query, ttl: view.ttl) })
      else { continue }
      view.rebind(to: fresh)
    }
    preloads.removeAll { !$0.active }
    for record in preloads {
      record.cleanupImpl =
        new.loop.call { new.context.preload(record.query, ttl: record.ttl) }
        .cleanup
    }
    old.close()
  }

  /// A view of the query's rows that stays current. Named queries are
  /// synced from the server; an unnamed one only sees local data.
  public func materialize<R: ZeroRow, S: QueryResult>(
    _ request: QueryRequest<R, S, Context>, ttl: TTL = .defaultTTL
  ) throws -> MaterializedView {
    try materialize(request.query(context), ttl: ttl)
  }

  /// A view of an unnamed query, which only sees local data.
  public func materialize<R: ZeroRow, S: QueryResult>(
    _ query: Query<R, S>, ttl: TTL = .defaultTTL
  ) throws -> MaterializedView {
    try materialize(query.any, ttl: ttl)
  }

  public func materialize(_ query: AnyQuery, ttl: TTL = .defaultTTL) throws -> MaterializedView {
    let view = try engine.loop.call { try engine.context.materialize(query, ttl: ttl) }
    view.query = query
    view.ttl = ttl
    if views.count >= 64, views.count.isMultiple(of: 64) {
      views.removeAll { $0.view?.isDestroyed ?? true }
    }
    views.append(WeakView(view))
    return view
  }

  /// The query's rows: what the replica holds now (`.unknown`), or once the
  /// server has confirmed them on this connection (`.complete`) or any
  /// (`.cached`).
  public func run<R: ZeroRow, S: QueryResult>(
    _ request: QueryRequest<R, S, Context>, type: RunType = .unknown, ttl: TTL? = nil
  ) async throws -> S {
    let data = try await run(request.query(context), type: type, ttl: ttl).value
    return try S(view: data)
  }

  public func run<R: ZeroRow, S: QueryResult>(
    _ query: Query<R, S>, type: RunType = .unknown, ttl: TTL? = nil
  ) async throws -> S {
    let data = try await run(query.any, type: type, ttl: ttl).value
    return try S(view: data)
  }

  public func run(
    _ query: AnyQuery, type: RunType = .unknown, ttl: TTL? = nil
  ) throws
    -> Deferred<ViewValue>
  {
    try engine.loop.call {
      try engine.context.run(query, type: QueryContext.RunType(rawValue: type.rawValue)!, ttl: ttl)
    }
  }

  /// Keeps the query's rows synced without a view, until `cleanup`.
  public func preload<R: ZeroRow, S: QueryResult>(
    _ request: QueryRequest<R, S, Context>, ttl: TTL = .defaultPreloadTTL
  ) -> Preload {
    preload(request.query(context), ttl: ttl)
  }

  public func preload(_ query: AnyQuery, ttl: TTL = .defaultPreloadTTL) -> Preload {
    let preload = engine.loop.call { engine.context.preload(query, ttl: ttl) }
    guard query.customQueryID != nil else { return preload }
    let record = PreloadRecord(query: query, ttl: ttl, cleanup: preload.cleanup)
    preloads.removeAll { !$0.active }
    preloads.append(record)
    return Preload(cleanup: record.cleanup, cached: preload.cached, complete: preload.complete)
  }

  /// Runs a mutator on the client now and sends it to the server.
  @discardableResult
  public func mutate(_ request: MutateRequest<Context>) -> MutatorResult {
    engine.loop.call { engine.mutate(name: request.name, args: request.args) }
  }

  /// The app came to the foreground, or went to the background.
  func setVisible(_ visible: Bool) {
    engine.loop.call { engine.setVisible(visible) }
  }

  /// The network may be back: reconnect without waiting for the backoff.
  func reconnectNow() {
    engine.loop.call { engine.reconnectNow() }
  }

  /// Calls `done` once the server has confirmed every pending mutation,
  /// or when it can't.
  func whenPendingSettled(_ done: @escaping () -> Void) {
    engine.loop.call { engine.whenPendingSettled(done) }
  }

  /// Waits for the store's queued writes.
  func flushStorage() {
    engine.database.flush()
  }

  /// Closes the connection. Pending mutations and queries fail, and views
  /// stop updating.
  public func close() {
    #if canImport(Darwin)
      lifecycle?.stop()
      lifecycle = nil
    #endif
    engine.loop.call { engine.close() }
  }
}

extension Zero: LifecycleClient {}

/// The socket factory a client uses by default: `URLSessionTransport` on
/// Apple platforms, and none elsewhere.
@MainActor
public func platformSocketFactory() -> (any SocketFactory)? {
  #if canImport(Darwin)
    TransportSocketFactory(URLSessionTransport())
  #else
    nil
  #endif
}

@MainActor
private struct WeakView {
  weak var view: MaterializedView?
  init(_ view: MaterializedView) { self.view = view }
}

/// A preload that moves to a new client with its `Zero`.
@MainActor
private final class PreloadRecord {
  let query: AnyQuery
  let ttl: TTL
  var cleanupImpl: () -> Void
  private(set) var active = true

  init(query: AnyQuery, ttl: TTL, cleanup: @escaping () -> Void) {
    self.query = query
    self.ttl = ttl
    self.cleanupImpl = cleanup
  }

  func cleanup() {
    guard active else { return }
    active = false
    cleanupImpl()
  }
}

extension Zero where Context == Void {
  public convenience init(
    schema: ZeroSchema, mutators: [any Mutator<Void>] = [], options: ZeroOptions,
    scheduler: any Scheduler = SystemScheduler(),
    socketFactory: (any SocketFactory)? = platformSocketFactory()
  ) throws {
    try self.init(
      schema: schema, context: (), mutators: mutators, options: options, scheduler: scheduler,
      socketFactory: socketFactory)
  }
}

/// How long `run` waits (`RunOptions.type`).
public enum RunType: String, Sendable {
  case unknown, cached, complete
}

/// The connection to zero-cache (`Connection` in
/// `packages/zero-client/src/client/connection.ts`). Observable: SwiftUI
/// views and UIKit and AppKit updates that read `state` follow it.
@Observable
@MainActor
public final class Connection {
  @ObservationIgnored private var engine: Engine
  public private(set) var state: ConnectionState
  /// Why the app must be updated to sync, once the server said so
  /// (`onUpdateNeeded`). Reconnecting doesn't help: the app should ask the
  /// user to update.
  public internal(set) var updateNeeded: UpdateNeededReason?
  @ObservationIgnored private var listeners: [(id: Int, listener: (ConnectionState) -> Void)] =
    []
  @ObservationIgnored private var nextID = 0
  @ObservationIgnored private var unsubscribe: () -> Void = {}

  init(_ engine: Engine) {
    self.engine = engine
    state = ConnectionState(engine.connectionManager.state)
    bind(engine)
  }

  /// Follows the state of `engine`, a new client.
  func bind(_ engine: Engine) {
    unsubscribe()
    self.engine = engine
    unsubscribe = engine.connectionManager.subscribe { [weak self] s in self?.update(s) }
    if ConnectionState(engine.connectionManager.state) != state {
      update(engine.connectionManager.state)
    }
  }

  private func update(_ s: ConnectionManager.State) {
    state = ConnectionState(s)
    for (_, l) in listeners { l(state) }
  }

  /// Calls `listener` on every change of `state`.
  @discardableResult
  public func subscribe(_ listener: @escaping (ConnectionState) -> Void) -> () -> Void {
    let id = nextID
    nextID += 1
    listeners.append((id, listener))
    return { [weak self] in self?.listeners.removeAll { $0.id == id } }
  }

  /// Resumes connecting from `needsAuth` or `error`. With `auth`, sets the
  /// token first; when already connected, the server gets the new token.
  public func connect(auth: String? = nil) {
    engine.loop.call { engine.connect(auth: auth.map { .some($0) }) }
  }
}

/// A `Zero` of any context, for code that needs only what doesn't depend
/// on it, such as the SwiftUI environment.
@MainActor
public protocol AnyZero: AnyObject, Sendable {
  var connection: Connection { get }
  var clientID: String { get }
  var clientGroupID: String { get }
  var userID: String? { get }
  var closed: Bool { get }
  func close()
}

extension Zero: AnyZero {}
