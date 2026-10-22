/// `RUN_LOOP_INTERVAL_MS`: the backoff before retrying a failed connection.
let runLoopIntervalMs: Double = 5_000
/// `CONNECT_TIMEOUT_MS`: each phase of a connection attempt gets this long.
let connectTimeoutMs: Double = 10_000

/// The options of a Zero client (`ZeroOptions` in
/// `packages/zero-client/src/client/options.ts`).
public struct ZeroOptions: Sendable {
  /// The zero-cache URL, `http` or `https`. Without one the client runs
  /// locally and never connects.
  public var cacheURL: String?
  /// The user the client syncs for. Omit it for a logged-out client.
  public var userID: String?
  /// The auth token zero-cache and the API server see.
  public var auth: String?
  /// Distinguishes local stores of the same user and URLs.
  public var storageKey: String?
  public var pingTimeoutMs: Double = 5_000
  public var disconnectTimeoutMs: Double = 60_000
  /// How long query changes are batched before they are sent.
  public var queryChangeThrottleMs: Double = 10
  /// The longest `Sec-WebSocket-Protocol` header the client sends.
  /// `initConnection` is sent as a message instead when it doesn't fit.
  public var maxHeaderLength: Int = 8 * 1024
  public var mutateURL: String?
  public var mutateHeaders: [String: String]?
  public var queryURL: String?
  public var queryHeaders: [String: String]?
  /// The server needs a newer client. Defaults to doing nothing.
  public var onUpdateNeeded: (@MainActor @Sendable (UpdateNeededReason) -> Void)?
  /// The server no longer has the client's state; the local store was reset.
  public var onClientStateNotFound: (@MainActor @Sendable () -> Void)?
  /// Where the replica is kept between launches.
  public var storage: ZeroStorage = .sqlite()
  /// The storage failed, the first time it does: the client keeps running
  /// on memory and stops writing to disk. Defaults to doing nothing.
  public var onStorageFailure: (@MainActor @Sendable (StorageFailure) -> Void)?
  /// The least severe messages logged.
  public var logLevel: LogLevel = .warn
  /// Where messages are logged. Defaults to `DefaultLogSink`.
  public var logSink: (any LogSink)?
  /// How long the app is in the background before the client disconnects
  /// (`hiddenTabDisconnectDelay`). The Apple layer pushes the pending
  /// mutations before it reports the app hidden, so it disconnects at once.
  var hiddenDisconnectDelayMs: Double = 0

  public init(
    cacheURL: String? = nil, userID: String? = nil, auth: String? = nil,
    storageKey: String? = nil, storage: ZeroStorage = .sqlite()
  ) {
    self.cacheURL = cacheURL
    self.userID = userID
    self.auth = auth
    self.storageKey = storageKey
    self.storage = storage
  }
}

/// Why the client must be updated (`UpdateNeededReason`).
public struct UpdateNeededReason: Sendable, Hashable {
  /// `VersionNotSupported`, `SchemaVersionNotSupported`, or `NewClientGroup`.
  public var type: String
  public var message: String?

  package var json: JSONValue {
    var o: JSONObject = ["type": .string(type)]
    if let message { o["message"] = .string(message) }
    return .object(o)
  }
}

/// A mutator bound to the app's context: runs it in a transaction.
typealias MutatorFunction = @MainActor (Transaction, JSONValue?) throws -> JSONValue?

/// The client engine (`Zero` in `packages/zero-client/src/client/zero.ts`,
/// with the parts of Replicache it uses): the run loop that keeps a
/// connection, the replica and its pending mutations, and the query and
/// mutation bookkeeping. Everything runs synchronously on the main actor;
/// see `EventLoop`.
@MainActor
final class Engine {
  let schema: ZeroSchema
  let options: ZeroOptions
  let clientID: String
  let clientGroupID: String
  let profileID: String
  let userID: String?
  let loop: EventLoop
  let scheduler: LoopScheduler
  let log: Log

  isolated deinit {
    loop.stop()
    for t in lifecycleTimers { t.cancel() }
    if !repClosed {
      database.removeWriteFailureListener(clientID)
      database.clientStopped(clientID)
      database.release()
    }
  }
  private let socketFactory: (any SocketFactory)?
  private let socketOrigin: String?
  private let mutators: [String: MutatorFunction]
  private let clientSchema: ClientSchema
  private let clientToServer: NameMapper

  let store: ReplicaStore
  let database: ReplicaDatabase
  private var lifecycleTimers: [any ScheduledTimer] = []
  let ivmMain: IVMBranch
  private(set) var context: QueryContext!
  let connectionManager: ConnectionManager
  private(set) var mutationTracker: MutationTracker!
  private(set) var queryManager: QueryManager!
  private var pokeHandler: PokeHandler!

  private var auth: String?
  private var repClosed = false
  private var socket: SocketHandle?
  private var connectCookie: JSONValue = .null
  private var initConnectionQueries: OrderedMap<JSKey, UpQueriesPatchOp>?
  /// Deleted clients still to tell the server about on this connection.
  private var deletedClients: DeleteClientsBody?
  private var lastMutationIDReceived: Double = 0
  private var lastMutationIDsSent: [String: Double] = [:]
  private var additionalConnectParams: [(String, String)]?
  private var currentAttempt: ConnectAttempt?
  private var connectionReady = ReadyPromise()
  private var recentSentMessages: [JSONValue] = []
  private var mutationRejection: ConnectionError?
  private var clientGroupDisabled = false

  // The run loop's current wait. Each wait gets a new token, so a wait that
  // was superseded does nothing when it ends.
  private var loopToken = 0
  private var abortPingTimeout: () -> Void = {}
  private var onPong: () -> Void = {}

  // The push loop (Replicache's push `ConnectionLoop`).
  private var pushTimer: (any ScheduledTimer)?
  private var pushInFlight = false
  private var pushRequested = false
  private var pushRequestCounter = 0

  init(
    schema: ZeroSchema, options: ZeroOptions, mutators: [String: MutatorFunction],
    scheduler base: any Scheduler, socketFactory: (any SocketFactory)?, visible: Bool = true
  ) throws {
    self.visible = visible
    if options.userID == "" {
      throw ConnectionError.client(
        .internal,
        message:
          "ZeroOptions.userID should not be empty. Omit it entirely for logged-out clients.")
    }
    try Self.checkAuthValid(options.userID, options.auth)
    self.schema = schema
    self.options = options
    self.userID = options.userID
    self.mutators = mutators
    self.socketFactory = socketFactory
    self.socketOrigin = try options.cacheURL.map(Self.socketOrigin)
    self.loop = EventLoop(scheduler: base)
    self.scheduler = LoopScheduler(base, loop)
    self.clientID = randomID()
    self.log = Log(level: options.logLevel, sink: options.logSink ?? DefaultLogSink())
      .with("clientID", clientID)
    self.clientSchema = schema.clientSchema
    self.clientToServer = NameMapper.clientToServer(schema)

    // Open the database and join a client group (`initClientV6`).
    let name = ReplicaDatabase.name(
      userID: options.userID, storageKey: options.storageKey ?? "",
      mutateURL: options.mutateURL, queryURL: options.queryURL,
      schemaHash: clientSchema.hash)
    var (database, openFailure) = ReplicaDatabase.open(options.storage, name: name)
    var joined: (clientGroupID: String, group: StoredGroup)
    do {
      joined = try database.initClient(
        clientID, mutatorNames: mutators.keys.sorted(), now: base.now)
    } catch {
      // The client group can't be read: run on memory, as when the open
      // fails.
      database.release()
      database = ReplicaDatabase.memory(name: name)
      openFailure = error
      joined = try database.initClient(
        clientID, mutatorNames: mutators.keys.sorted(), now: base.now)
    }
    let (clientGroupID, group) = joined
    self.database = database
    self.clientGroupID = clientGroupID
    self.profileID = database.profileID
    self.store = ReplicaStore(
      clientID: clientID,
      snapshot: ReplicaStore.Snapshot(
        map: group.entries, cookie: group.cookie, lastMutationIDs: group.lastMutationIDs),
      pending: group.mutations.map {
        ReplicaStore.PendingMutation(
          id: $0.id, clientID: $0.clientID, name: $0.name, args: $0.args,
          timestamp: $0.timestamp, writes: [])
      })
    self.ivmMain = IVMBranch(schema: schema)
    let loop = self.loop
    self.connectionManager = ConnectionManager(
      disconnectTimeout: options.disconnectTimeoutMs, scheduler: scheduler,
      microtask: { loop.microtask($0) })

    _ = connectionManager.subscribe { [weak self] state in
      if case .closed(let reason) = state { self?.queryManager.handleClosed(reason) }
    }
    mutationTracker = MutationTracker(
      ackMutations: { [weak self] id in self?.sendIfConnected(.ackMutationResponses(id)) },
      onFatalError: { [weak self] error in self?.disconnect(error) })
    mutationTracker.setClientID(clientID)
    context = QueryContext(
      branch: ivmMain,
      addCustomQuery: { [weak self] ast, id, ttl, got in
        self?.queryManager.addCustom(ast, name: id.name, args: id.args, ttl: ttl, gotCallback: got)
          ?? {}
      },
      updateCustomQuery: { [weak self] id, ttl in
        self?.queryManager.updateCustom(name: id.name, args: id.args, ttl: ttl)
      },
      batchViewUpdates: { $0() },
      microtask: { loop.microtask($0) }, log: log,
      closedReason: { [weak self] in
        guard case .closed(let reason) = self?.connectionManager.state else { return nil }
        return reason
      })
    _ = connectionManager.subscribe { [weak self] state in self?.onConnectionStateChange(state) }
    onConnectionStateChange(connectionManager.state)
    queryManager = QueryManager(
      clientID: clientID, schema: schema, mutationTracker: mutationTracker, scheduler: scheduler,
      queryChangeThrottleMs: options.queryChangeThrottleMs, recentQueriesMaxSize: 0,
      send: { [weak self] body in self?.sendIfConnected(.changeDesiredQueries(body)) },
      onFatalError: { [weak self] error in self?.disconnect(error) })
    pokeHandler = PokeHandler(
      clientID: clientID, schema: schema, scheduler: scheduler,
      apply: { [weak self] poke in try self?.applyPoke(poke) },
      onApplied: { [weak self] poke in
        guard let self else { return }
        defer { self.settlePendingWaiters() }
        guard let lmid = poke.lastMutationIDChanges[JSKey(self.clientID)] else { return }
        try self.mutationTracker.lmidAdvanced(lmid)
      },
      onPokeError: { [weak self] message in self?.onPokeError(message) })

    // Load the snapshot into the sources and replay the pending mutations
    // on it.
    ivmMain.apply(KVDiff.between(KVMap(), store.snapshot.map))
    if !store.pending.isEmpty {
      let (main, replayed, touched, _) = replay(
        store.pending, over: store.snapshot.map, branch: ivmMain.fork())
      ivmMain.apply(
        store.advance(to: store.snapshot, main: main, pending: replayed, touched: touched))
    }
    database.addWriteFailureListener(clientID) { [weak self] failure in
      self?.storageFailed(failure)
    }
    // Another client of the group stopped (the `onDelete` of
    // `makeActiveClientsManager`): tell the server, if connected.
    database.clientStarted(clientID, clientGroupID: clientGroupID) { [weak self] id in
      guard let self else { return }
      self.loop.dispatch {
        self.sendIfConnected(.deleteClients(DeleteClientsBody(clientIDs: [id])))
      }
    }
    if let openFailure {
      log.warn(
        "Local store storage failed (\(openFailure.kind.rawValue)) opening \(name); "
          + "running this instance in memory: \(openFailure.message)")
      loop.microtask { [weak self] in self?.storageFailed(openFailure, opening: true) }
    }
    startLifecycle()

    // The replica is loaded: report its got queries and mutation results,
    // as the first runs of the store watches do, then build the pipelines.
    fireWatchers(KVDiff.between(KVMap(), store.main), initial: true)
    context.markPipelinesReady()
    loop.microtask { [weak self] in self?.startRunLoop() }
  }

  static func checkAuthValid(_ userID: String?, _ auth: String?) throws {
    if userID == nil, let auth, !auth.isEmpty {
      throw ConnectionError.client(
        .internal, message: "ZeroOptions.userID is required when auth is set.")
    }
  }

  /// The WebSocket origin of the zero-cache URL (`getServer`, `toWSString`).
  static func socketOrigin(_ url: String) throws -> String {
    let scheme: String
    if url.hasPrefix("http://") {
      scheme = "ws://"
    } else if url.hasPrefix("https://") {
      scheme = "wss://"
    } else {
      throw ConnectionError.client(
        .internal, message: "ZeroOptions.cacheURL must use the \"http\" or \"https\" scheme.")
    }
    var rest = String(url.drop { $0 != "/" }.dropFirst(2))
    if !rest.contains("/") { rest += "/" }
    return scheme + rest
  }

  // MARK: - Sending

  private func sendIfConnected(_ message: Upstream) {
    if connectionManager.is(.connected) { send(message) }
  }

  private func send(_ message: Upstream) {
    guard let socket else { return }
    let text = message.text
    if message.tag != "ping" {
      if recentSentMessages.count >= 5 { recentSentMessages.removeFirst() }
      recentSentMessages.append([
        "type": .string(message.tag), "size": .number(Double(text.utf16.count)),
        "snippet": .string(String(text.utf16.prefix(200)) ?? text),
      ])
    }
    socket.send(text)
  }

  // MARK: - The run loop

  private func startRunLoop() {
    guard socketOrigin != nil else {
      connectionManager.disconnected(
        .client(.noSocketOrigin, message: "No server socket origin provided"))
      return
    }
    setAuth(options.auth)
    connectionManager.initialized()
    iterate()
  }

  private func nextLoopToken() -> Int {
    loopToken += 1
    return loopToken
  }

  /// One turn of the JS run loop: acts on the connection status.
  private func iterate() {
    guard connectionManager.shouldContinueRunLoop else { return }
    let token = nextLoopToken()
    switch connectionManager.state.status {
    case .connecting, .disconnected:
      guard visible else {
        // Wait for the app to come back, or for a state change.
        onVisibility(true, token) { [weak self] in self?.beginAttempt() }
        onStateChange(token) { [weak self] state in self?.resumeAfterStateChange(state) }
        return
      }
      // The app is visible, which wins the race with a state change.
      loop.microtask { [weak self] in
        guard let self, self.loopToken == token else { return }
        self.beginAttempt()
      }
    case .connected:
      waitWhileConnected(token)
    case .needsAuth, .error:
      log.info(
        "Run loop paused in \(connectionManager.state.status.rawValue) state. "
          + "Call zero.connection.connect() to resume.")
      waitForConnectRequest(token)
    case .closed:
      return
    case .initializing:
      preconditionFailure("The run loop runs once initialized")
    }
  }

  /// Ends the current wait of the run loop with `error`
  /// (the `catch` of the JS run loop).
  private func loopFailed(_ error: ConnectionError) {
    let (transition, reason) = ConnectionTransition.of(error)
    if !connectionManager.is(.connected), error.clientKind != .clientClosed {
      // Only errors that stop the run loop are errors. Reconnects and auth
      // failures are routine.
      var level: LogLevel
      switch transition {
      case .error: level = .error
      case .disconnected, .closed: level = .info
      case .none, .needsAuth: level = .warn
      }
      if error.serverKind == .clientNotFound { level = .warn }
      log(level, "Failed to connect: \(error.message)")
    }
    var backoffMs = runLoopIntervalMs
    var sleepMs: Double?
    switch transition {
    case .none:
      if let backoff = reason.backoff {
        if let min = backoff.minBackoffMs { backoffMs = max(backoffMs, min) }
        if let max = backoff.maxBackoffMs { backoffMs = Swift.min(backoffMs, max) }
        additionalConnectParams = backoff.reconnectParams.map { params in
          params.sorted { $0.key < $1.key }.map { ($0.key, $0.value) }
        }
      }
      sleepMs = backoffMs
    case .needsAuth:
      connectionManager.needsAuth(reason)
    case .error:
      connectionManager.error(reason)
    case .disconnected:
      connectionManager.disconnected(reason)
    case .closed:
      break
    }
    let token = nextLoopToken()
    if let sleepMs, sleepMs > 0 {
      let timer = scheduler.schedule(after: sleepMs) { [weak self] in
        guard let self, self.loopToken == token else { return }
        self.backoff = nil
        self.iterate()
      }
      backoff = (token, timer)
    } else {
      loop.microtask { [weak self] in
        guard let self, self.loopToken == token else { return }
        self.iterate()
      }
    }
  }

  /// `throwIfConnectionError`: whether a state the run loop wakes up to
  /// stops it. Dropped sockets and timeouts just reconnect.
  private static func connectionError(_ state: ConnectionManager.State) -> ConnectionError? {
    switch state {
    case .needsAuth(let r), .error(let r), .closed(let r): return r
    case .connecting(_, _, let r?), .disconnected(let r):
      switch r.clientKind {
      case .connectTimeout?, .abruptClose?, .cleanClose?, .hidden?: return nil
      default: return r
      }
    default: return nil
    }
  }

  /// Wakes the loop on the next state change, unless the wait is over.
  private func onStateChange(_ token: Int, _ body: @escaping (ConnectionManager.State) -> Void) {
    connectionManager.onNextStateChange { [weak self] state in
      guard let self, self.loopToken == token else { return }
      body(state)
    }
  }

  private func resumeAfterStateChange(_ state: ConnectionManager.State) {
    if let error = Self.connectionError(state) {
      loopFailed(error)
    } else {
      iterate()
    }
  }

  /// Connected: ping after `pingTimeoutMs` without poke traffic.
  private func waitWhileConnected(_ token: Int) {
    let ready = connectionReady
    var timer: (any ScheduledTimer)?
    let end = { [weak self] in
      guard let self else { return }
      // The `finally` of the connected case.
      if !self.connectionManager.is(.connected), self.connectionReady === ready {
        self.connectionReady = ReadyPromise()
      }
    }
    abortPingTimeout = { [weak self] in
      timer?.cancel()
      self?.loop.microtask {
        guard let self, self.loopToken == token else { return }
        end()
        self.iterate()
      }
    }
    timer = scheduler.schedule(after: options.pingTimeoutMs) { [weak self] in
      guard let self, self.loopToken == token else { return }
      self.ping(token, end)
    }
    pingNow = { [weak self] in
      guard let self, self.loopToken == token else { return }
      timer?.cancel()
      self.ping(token, end)
    }
    onVisibility(false, token) { [weak self] in
      guard let self else { return }
      timer?.cancel()
      self.disconnect(
        .client(.hidden, message: "Connection closed because tab was hidden"))
      end()
      self.iterate()
    }
    onStateChange(token) { [weak self] state in
      end()
      self?.resumeAfterStateChange(state)
    }
  }

  private func ping(_ token: Int, _ end: @escaping () -> Void) {
    let token = nextLoopToken()
    send(.ping)
    onPong = { [weak self] in
      self?.loop.microtask {
        guard let self, self.loopToken == token else { return }
        end()
        self.iterate()
      }
    }
    _ = scheduler.schedule(after: options.pingTimeoutMs) { [weak self] in
      guard let self, self.loopToken == token else { return }
      let error = ConnectionError.client(.pingTimeout, message: "Server ping request failed")
      self.disconnect(error)
      end()
      self.loopFailed(error)
    }
    onStateChange(token) { [weak self] state in
      end()
      self?.resumeAfterStateChange(state)
    }
  }

  /// needs-auth and error: wait for `connect()`.
  private func waitForConnectRequest(_ token: Int) {
    connectionManager.onConnectRequest { [weak self] in
      guard let self, self.loopToken == token else { return }
      self.connectionManager.resumeFromConnectRequest()
      self.iterate()
    }
    onStateChange(token) { [weak self] _ in self?.iterate() }
  }

  // MARK: - The app's lifecycle

  /// The run loop's backoff before it reconnects.
  private var backoff: (token: Int, timer: any ScheduledTimer)?
  /// Pings the server now, when the run loop waits while connected.
  private var pingNow: () -> Void = {}
  /// Trails the app's visibility by `hiddenDisconnectDelayMs` when it goes
  /// to the background (`DocumentVisibilityWatcher`).
  private(set) var visible = true
  private var hiddenTimer: (any ScheduledTimer)?
  private var visibilityWaiters: [(visible: Bool, token: Int, waiter: () -> Void)] = []
  private var pendingWaiters: [() -> Void] = []

  /// The app came to the foreground, or went to the background. In the
  /// background the client disconnects and waits to come back.
  func setVisible(_ visible: Bool) {
    hiddenTimer?.cancel()
    hiddenTimer = nil
    if visible {
      setVisibilityState(true)
    } else {
      hiddenTimer = scheduler.schedule(after: options.hiddenDisconnectDelayMs) { [weak self] in
        self?.hiddenTimer = nil
        self?.setVisibilityState(false)
      }
    }
  }

  private func setVisibilityState(_ visible: Bool) {
    guard visible != self.visible else { return }
    self.visible = visible
    let ready = visibilityWaiters.filter { $0.visible == visible }
    visibilityWaiters.removeAll { $0.visible == visible }
    for w in ready { w.waiter() }
  }

  /// Calls `body` in a microtask once the app's visibility is `visible`,
  /// unless the run loop's wait `token` is over by then.
  private func onVisibility(_ visible: Bool, _ token: Int, _ body: @escaping () -> Void) {
    let waiter: () -> Void = { [weak self] in
      self?.loop.microtask {
        guard let self, self.loopToken == token else { return }
        // The wait is over: a state change no longer ends it.
        _ = self.nextLoopToken()
        body()
      }
    }
    if self.visible == visible {
      waiter()
    } else {
      visibilityWaiters.removeAll { $0.token != loopToken }
      visibilityWaiters.append((visible, token, waiter))
    }
  }

  /// The network may be back: retry now instead of after the backoff, and
  /// check that an open connection still works.
  func reconnectNow() {
    if let backoff, backoff.token == loopToken {
      backoff.timer.cancel()
      self.backoff = nil
      iterate()
    } else if connectionManager.is(.connected) {
      pingNow()
    }
  }

  /// Calls `done` once the server has confirmed every pending mutation, or
  /// right away when it can't: the client isn't connected, or the group is
  /// disabled.
  func whenPendingSettled(_ done: @escaping () -> Void) {
    if store.pending.isEmpty || !connectionManager.is(.connected) || clientGroupDisabled
      || repClosed
    {
      done()
      return
    }
    pendingWaiters.append(done)
  }

  private func settlePendingWaiters() {
    guard !pendingWaiters.isEmpty else { return }
    if store.pending.isEmpty || !connectionManager.is(.connected) || clientGroupDisabled
      || repClosed
    {
      let waiters = pendingWaiters
      pendingWaiters = []
      for w in waiters { w() }
    }
  }

  // MARK: - Connecting

  final class ConnectAttempt {
    var events: [(Double, String)] = []
    var aborted = false
    var abortReason: ConnectionError?
    var timeout: (any ScheduledTimer)?
    var finished = false
    var onConnected: (() -> Void)?
  }

  private func addConnectEvent(_ attempt: ConnectAttempt, _ event: String) {
    attempt.events.append((scheduler.now, event))
  }

  private func armConnectTimeout(_ attempt: ConnectAttempt, ack: Bool) {
    attempt.timeout?.cancel()
    guard !attempt.aborted else { return }
    attempt.timeout = scheduler.schedule(after: connectTimeoutMs) { [weak self, weak attempt] in
      guard let self, let attempt else { return }
      let events = JSONValue.array(
        attempt.events.map { [.string(isoDateString($0.0)), .string($0.1)] })
      let seconds = JSNumber.format(connectTimeoutMs / 1000)
      let message =
        (ack
          ? "Server did not acknowledge the connection within \(seconds) seconds. "
          : "Connection setup timed out after \(seconds) seconds. ")
        + "Connect events: \(JSONSerializer.stringify(events))"
      self.disconnect(.client(.connectTimeout, message: message))
    }
  }

  private func beginAttempt() {
    precondition(currentAttempt == nil, "Connection attempt already active")
    let attempt = ConnectAttempt()
    currentAttempt = attempt
    addConnectEvent(attempt, "starting the connection")
    armConnectTimeout(attempt, ack: false)
    connectAttempt(attempt)
  }

  /// `#connectAttempt`: opens a socket and waits for `connected`.
  private func connectAttempt(_ attempt: ConnectAttempt) {
    if repClosed {
      finishAttempt(attempt, nil)
      return
    }
    guard let socketOrigin, let socketFactory else {
      finishAttempt(attempt, .client(.internal, message: "No server provided"))
      return
    }
    precondition(
      connectionManager.is(.disconnected) || connectionManager.is(.connecting),
      "connectAttempt() called from invalid state")
    let wsid = randomID()
    connectionManager.connecting()
    addConnectEvent(attempt, "reading the cookie")
    let connectCookie = store.cookie
    addConnectEvent(attempt, "reading the client group ID")
    addConnectEvent(attempt, "initializing active clients")
    if attempt.aborted {
      finishAttempt(attempt, attempt.abortReason)
      return
    }

    // `createSocket`.
    addConnectEvent(attempt, "reading the profile ID")
    let url = ConnectionURL(
      socketOrigin: socketOrigin, clientID: clientID, clientGroupID: clientGroupID,
      userID: userID,
      baseCookie: connectCookie.stringValue ?? connectCookie.numberValue.map(JSNumber.format),
      timestamp: scheduler.monotonicNow, lastMutationID: lastMutationIDReceived, wsid: wsid,
      profileID: profileID, additionalParams: additionalConnectParams ?? [])
    addConnectEvent(attempt, "reading deleted clients")
    var deleted = deletedClientsBody()
    addConnectEvent(attempt, "reading desired queries")
    var queriesPatch: OrderedMap<JSKey, UpQueriesPatchOp>? = queryManager.queriesPatch(store: store)
    let initConnection = InitConnectionBody(
      desiredQueriesPatch: queriesPatch!.entries.map(\.value),
      clientSchema: connectCookie.isNull ? clientSchema : nil, deleted: deleted,
      userPushURL: options.mutateURL, userPushHeaders: options.mutateHeaders,
      userQueryURL: options.queryURL, userQueryHeaders: options.queryHeaders,
      activeClients: [clientID]
        + database.activeClients(ofGroup: clientGroupID).filter {
          $0 != clientID
        })
    let authToken = auth.flatMap { $0.isEmpty ? nil : $0 }
    var secProtocol = SecProtocols.encode(initConnection: initConnection, authToken: authToken)
    if secProtocol.utf8.count > options.maxHeaderLength {
      secProtocol = SecProtocols.encode(initConnection: nil, authToken: authToken)
      queriesPatch = nil
      if secProtocol.utf8.count > options.maxHeaderLength {
        log.warn(
          "Encoded auth token length (\(secProtocol.utf8.count)) exceeds "
            + "ZeroOptions.maxHeaderLength (\(options.maxHeaderLength)). This may "
            + "cause connection failures.")
      }
    } else {
      deleted = nil
    }
    addConnectEvent(attempt, "creating the WebSocket")
    let handle = SocketHandle(engine: self)
    handle.connection = socketFactory.open(
      url: url.string, secProtocol: secProtocol, events: handle)

    if attempt.aborted {
      handle.close(code: nil)
      finishAttempt(attempt, attempt.abortReason)
      return
    }
    if repClosed {
      handle.close(code: nil)
      finishAttempt(attempt, nil)
      return
    }
    self.connectCookie = connectCookie
    self.initConnectionQueries = queriesPatch
    self.deletedClients = deleted
    socket = handle
    addConnectEvent(attempt, "waiting for the server acknowledgement")
    armConnectTimeout(attempt, ack: true)
    attempt.onConnected = { [weak self, weak attempt] in
      guard let self, let attempt else { return }
      if attempt.aborted {
        self.finishAttempt(attempt, attempt.abortReason)
        return
      }
      self.mutationTracker.onConnected(self.lastMutationIDReceived)
      self.requestPush(now: true)
      self.finishAttempt(attempt, nil)
    }
  }

  /// The run loop's handling of a finished attempt; `error` nil for success.
  private func finishAttempt(_ attempt: ConnectAttempt, _ error: ConnectionError?) {
    guard !attempt.finished else { return }
    attempt.finished = true
    attempt.timeout?.cancel()
    if currentAttempt === attempt { currentAttempt = nil }
    let ready = connectionReady
    if let error {
      if !attempt.aborted { disconnect(error) }
      ready.reject(loop)
      if connectionReady === ready { connectionReady = ReadyPromise() }
      loopFailed(error)
      return
    }
    ready.resolve(loop)
    additionalConnectParams = nil
    if let error = Self.connectionError(connectionManager.state) {
      loopFailed(error)
    } else {
      iterate()
    }
  }

  /// `#disconnect`: closes the socket and moves the connection status.
  func disconnect(_ reason: ConnectionError, closeCode: Int? = nil) {
    if let attempt = currentAttempt, !attempt.aborted {
      attempt.abortReason = reason
      attempt.aborted = true
      attempt.timeout?.cancel()
      loop.microtask { [weak self] in self?.finishAttempt(attempt, reason) }
    }
    if connectionManager.is(.closed) { return }
    socket?.close(code: closeCode)
    socket = nil
    lastMutationIDsSent.removeAll()
    pokeHandler.handleDisconnect()
    queryManager.clearGotQueriesAuthoritative()
    let (transition, r) = ConnectionTransition.of(reason)
    switch transition {
    case .needsAuth: connectionManager.needsAuth(r)
    case .error: connectionManager.error(r)
    case .disconnected: connectionManager.disconnected(r)
    case .closed: connectionManager.closed()
    case .none: if !connectionManager.isInTerminalState { connectionManager.connecting(r) }
    }
  }

  // MARK: - Messages

  fileprivate func socketDidReceive(_ handle: SocketHandle, _ message: SocketMessage) {
    guard handle === socket else { return }
    loop.dispatch { onMessage(message) }
  }

  fileprivate func socketDidClose(
    _ handle: SocketHandle, code: Int, reason: String, wasClean: Bool
  ) {
    guard handle === socket else { return }
    loop.dispatch {
      // 1005 and 1006 are how platforms report a close without a frame.
      if code <= 1001 || code == 1005 || code == 1006 {
        log.info("Got socket close event: code=\(code) reason=\(reason) wasClean=\(wasClean)")
      } else {
        log.error(
          "Got unexpected socket close event: code=\(code) reason=\(reason) wasClean=\(wasClean)")
      }
      if code == 1009 {
        var message =
          "A WebSocket message exceeded the server message size limit. "
          + "This is usually caused by a mutation's args containing a large "
          + "value such as a base64-encoded image. Consider uploading "
          + "large files to object storage and storing only the URL in Zero."
        if !recentSentMessages.isEmpty {
          message +=
            "\nRecent sent messages:\n"
            + Self.prettyRecentMessages(recentSentMessages)
        }
        disconnect(.client(.invalidMessage, message: message))
        disableClientGroup()
        loop.microtask { [weak self] in self?.clientStateNotFound() }
      } else {
        disconnect(
          wasClean
            ? .client(.cleanClose, message: "WebSocket connection closed cleanly")
            : .client(.abruptClose, message: "WebSocket connection closed abruptly"))
      }
    }
  }

  private func onMessage(_ message: SocketMessage) {
    if repClosed { return }
    let text: String
    switch message {
    case .binary(let bytes):
      guard bytes.first == PokeChunk.messageType else {
        disconnect(
          .client(
            .invalidMessage,
            message: "Unknown binary message type: \(bytes.first.map(String.init) ?? "undefined")"
          ))
        return
      }
      abortPingTimeout()
      pokeHandler.handlePokeChunk(bytes.dropFirst())
      return
    case .text(let t):
      text = t
    }
    let downstream: Downstream
    do {
      downstream = try Downstream(text: text)
    } catch {
      disconnect(
        .client(
          .invalidMessage,
          message: "Invalid message received from server: \(valitaMessage(error, text))\(text)"))
      return
    }
    do {
      switch downstream {
      case .connected(let body):
        loop.microtask { [weak self] in self?.handleConnected(body) }
      case .error(let body):
        handleErrorMessage(body)
      case .pong:
        onPong()
      case .pokeStart(let body):
        abortPingTimeout()
        pokeHandler.handlePokeStart(body)
      case .pokePart(let body):
        abortPingTimeout()
        if let lmid = pokeHandler.handlePokePart(body) { lastMutationIDReceived = lmid }
      case .pokeEnd(let body):
        abortPingTimeout()
        if let lmid = pokeHandler.handlePokeEnd(body)?.lastMutationIDChangeForSelf {
          lastMutationIDReceived = lmid
        }
      case .pull:
        abortPingTimeout()
      case .deleteClients(let body):
        database.confirmDeletedClients(
          clientIDs: body.clientIDs ?? [], clientGroupIDs: body.clientGroupIDs ?? [])
      case .pushResponse(let body):
        if case .error = body {
          log.error("Received an error response when pushing mutations: \(body.json)")
        }
        try mutationTracker.processPushResponse(body)
      case .transformError(let errors):
        queryManager.handleTransformErrors(errors)
      case .inspect:
        break
      }
    } catch {
      disconnect(.client(.internal, message: errorMessage(error)))
    }
  }

  private func handleConnected(_ body: ConnectedBody) {
    guard let attempt = currentAttempt else { return }
    addConnectEvent(attempt, "processing the server acknowledgement")
    lastMutationIDsSent.removeAll()
    let patch = queryManager.queriesPatch(store: store, lastPatch: initConnectionQueries)
    if patch.count > 0, initConnectionQueries != nil {
      send(
        .changeDesiredQueries(
          ChangeDesiredQueriesBody(desiredQueriesPatch: patch.entries.map(\.value))))
    } else if initConnectionQueries == nil {
      send(
        .initConnection(
          InitConnectionBody(
            desiredQueriesPatch: patch.entries.map(\.value),
            clientSchema: connectCookie.isNull ? clientSchema : nil, deleted: deletedClients,
            userPushURL: options.mutateURL, userPushHeaders: options.mutateHeaders,
            userQueryURL: options.queryURL, userQueryHeaders: options.queryHeaders)))
      deletedClients = nil
    }
    initConnectionQueries = nil
    if let deleted = deletedClients {
      send(.deleteClients(deleted))
      deletedClients = nil
    }
    connectionManager.connected()
    if let onConnected = attempt.onConnected {
      attempt.onConnected = nil
      loop.microtask(onConnected)
    }
  }

  private func handleErrorMessage(_ body: ErrorBody) {
    if body.kind == .mutationRateLimited {
      lastMutationIDsSent.removeAll()
      log.error("\(body.kind.rawValue) Mutation rate limited: \(body.message)")
      return
    }
    // Being sent elsewhere or later, a lost client group (which recovers),
    // and auth failures are routine; every other kind ends the connection.
    let error = ConnectionError.server(body)
    let routine = error.backoff != nil || body.kind == .clientNotFound || error.isAuthError
    log(routine ? .warn : .error, "\(body.kind.rawValue):\n\n\(body.message)")
    disconnect(error)
    switch body.kind {
    case .versionNotSupported:
      updateNeeded(UpdateNeededReason(type: body.kind.rawValue, message: body.message))
    case .schemaVersionNotSupported:
      disableClientGroup()
      loop.microtask { [weak self] in
        self?.updateNeeded(
          UpdateNeededReason(type: "SchemaVersionNotSupported", message: body.message))
      }
    case .clientNotFound:
      disableClientGroup()
      loop.microtask { [weak self] in self?.clientStateNotFound() }
    case .invalidConnectionRequestLastMutationID, .invalidConnectionRequestBaseCookie:
      // The server is behind the replica: drop it.
      database.drop()
      loop.microtask { [weak self] in self?.clientStateNotFound() }
    default:
      break
    }
  }

  private func onPokeError(_ message: String) {
    guard !connectionManager.is(.disconnected) else { return }
    if message.contains("unexpected base cookie") {
      log.debug("clearing due to \(message)")
      disconnect(
        .client(
          .unexpectedBaseCookie, message: "Server returned unexpected base cookie during sync"))
    } else {
      log.error("clearing due to unexpected poke error: \(message)")
      disconnect(.client(.internal, message: "Poke processing error: \(message)"))
    }
  }

  // MARK: - Auth

  func setAuth(_ auth: String?) {
    // A token without a user was rejected at init.
    self.auth = auth ?? ""
    if let auth, !auth.isEmpty {
      sendIfConnected(.updateAuth(UpdateAuthBody(auth: auth)))
    }
  }

  /// `connection.connect()`: resumes from needs-auth or error.
  func connect(auth: String??) {
    if case .some(let auth) = auth { setAuth(auth) }
    if case .disconnected(let r) = connectionManager.state, r.clientKind == .noSocketOrigin {
      log.error(
        "connect() called but the connection is disconnected due to a missing cacheURL. "
          + "No reconnect will be attempted.")
      return
    }
    guard connectionManager.isInTerminalState else { return }
    connectionManager.requestConnect()
  }

  /// What `Zero` does when the client can't sync any longer: by default
  /// `ZeroOptions.onClientStateNotFound`.
  var onClientStateNotFound: (() -> Void)?
  /// What `Zero` does when the app must be updated to sync, besides calling
  /// `ZeroOptions.onUpdateNeeded`.
  var onUpdateNeeded: ((UpdateNeededReason) -> Void)?

  private func clientStateNotFound() {
    if let onClientStateNotFound {
      onClientStateNotFound()
    } else {
      options.onClientStateNotFound?()
    }
  }

  private func updateNeeded(_ reason: UpdateNeededReason) {
    onUpdateNeeded?(reason)
    options.onUpdateNeeded?(reason)
  }

  // MARK: - Close

  var closed: Bool { repClosed }

  func close() {
    if repClosed { return }
    if !connectionManager.is(.disconnected) {
      disconnect(.client(.clientClosed, message: "Zero instance closed by user"), closeCode: 1000)
    }
    repClosed = true
    pushTimer?.cancel()
    pushTimer = nil
    for t in lifecycleTimers { t.cancel() }
    lifecycleTimers = []
    database.removeWriteFailureListener(clientID)
    database.clientStopped(clientID)
    database.release()
    connectionManager.closed()
  }

  // MARK: - Storage and the client lifecycle

  /// `HEARTBEAT_INTERVAL`.
  static let heartbeatIntervalMs: Double = 60_000
  /// `GC_INTERVAL`, of clients and of client groups.
  static let gcIntervalMs: Double = 5 * 60_000
  /// `CLIENT_MAX_INACTIVE_TIME`.
  static let clientMaxInactiveMs: Double = 24 * 60 * 60_000

  /// Starts the heartbeat and the garbage collection of clients and client
  /// groups.
  private func startLifecycle() {
    lifecycleTimers = [
      scheduler.scheduleRepeating(every: Self.heartbeatIntervalMs) { [weak self] in
        guard let self else { return }
        if !self.database.heartbeat(self.clientID, now: self.scheduler.now) {
          self.clientStateNotFound()
        }
      },
      scheduler.scheduleRepeating(every: Self.gcIntervalMs) { [weak self] in
        guard let self else { return }
        if let deleted = self.database.collectClients(
          keeping: self.clientID, now: self.scheduler.now, maxInactive: Self.clientMaxInactiveMs)
        {
          // `DeleteClientsManager.onClientsDeleted`.
          let ids = deleted.filter { $0.clientGroupID == self.clientGroupID }.map(\.clientID)
          self.sendIfConnected(.deleteClients(DeleteClientsBody(clientIDs: ids)))
        }
      },
      scheduler.scheduleRepeating(every: Self.gcIntervalMs) { [weak self] in
        self?.database.collectClientGroups()
      },
      scheduler.schedule(after: Self.initialCollectDatabasesDelayMs) { [weak self] in
        guard let self else { return }
        self.collectDatabases()
        self.lifecycleTimers.append(
          self.scheduler.scheduleRepeating(every: Self.collectDatabasesIntervalMs) {
            [weak self] in self?.collectDatabases()
          })
      },
    ]
  }

  /// `INITIAL_COLLECT_IDB_DELAY` and `COLLECT_IDB_INTERVAL`.
  static let initialCollectDatabasesDelayMs: Double = 5 * 60_000
  static let collectDatabasesIntervalMs: Double = 12 * 60 * 60_000

  /// Deletes the other databases of the directory that no client has used
  /// for twice the time a client may be inactive (`collectIDBDatabases`),
  /// such as those of an older schema.
  private func collectDatabases() {
    database.collectDatabases(now: scheduler.now, maxAge: 2 * Self.clientMaxInactiveMs)
  }

  /// The deleted clients of this client group, for `initConnection`
  /// (`convertDeletedClientsToBody`).
  private func deletedClientsBody() -> DeleteClientsBody? {
    let ids = database.deletedClients.filter { $0.clientGroupID == clientGroupID }.map(\.clientID)
    return ids.isEmpty ? nil : DeleteClientsBody(clientIDs: ids)
  }

  /// The server lost the client group: stop pushing, and keep later clients
  /// out of it.
  private func disableClientGroup() {
    clientGroupDisabled = true
    database.disableClientGroup(clientGroupID)
  }

  private var storageFailureReported = false

  private func storageFailed(_ failure: StorageFailure, opening: Bool = false) {
    guard !storageFailureReported else { return }
    storageFailureReported = true
    if !opening {
      // A full or failing disk is the device's condition, not a defect.
      log.warn(
        "Local store storage failed (\(failure.kind.rawValue)); persistence is stopped for "
          + "this instance: \(failure.message)")
    }
    options.onStorageFailure?(failure)
  }

  // MARK: - Mutations

  /// `MutatorProxy#onConnectionStateChange`.
  private func onConnectionStateChange(_ state: ConnectionManager.State) {
    defer { settlePendingWaiters() }
    switch state {
    case .disconnected(let r) where r.clientKind == .noSocketOrigin:
      mutationRejection = nil
    case .disconnected(let r), .error(let r), .closed(let r):
      mutationRejection = r
      mutationTracker.rejectAllOutstandingMutations(r)
    default:
      mutationRejection = nil
    }
  }

  /// `zero.mutate`, through the `MutatorProxy` and Replicache's `mutate`.
  func mutate(name: String, args: JSONValue?) -> MutatorResult {
    let client = Deferred<MutationOutcome>()
    let server = Deferred<MutationOutcome>()
    // JS throws this before it looks at the connection; here both outcomes
    // carry it, and nothing is written or pushed.
    guard let mutator = mutators[name] else {
      let message =
        "Mutator \"\(name)\" is not registered. "
        + "Mutators must be registered with the Zero constructor before use."
      log.error(message)
      client.resolve(.zeroError(message: message))
      server.resolve(.zeroError(message: message))
      return MutatorResult(client: client, server: server)
    }
    if let rejection = mutationRejection {
      log.warn(
        "Mutation \"\(name)\" rejected because Zero is \(rejection.stateDescription). "
          + "Details: \(rejection.message).")
      client.resolve(.zeroError(message: rejection.message))
      server.resolve(.zeroError(message: rejection.message))
      return MutatorResult(client: client, server: server)
    }
    let (ephemeralID, tracked) = mutationTracker.trackMutation()
    let log = self.log
    tracked.onSettle { result in
      switch result {
      case .success(let data): server.resolve(.success(data: data))
      case .failure(let error):
        let outcome = Self.outcome(error)
        Self.logFailure(log, name, outcome, on: "server")
        server.resolve(outcome)
      }
    }
    let mutationID = store.nextMutationID(for: clientID)
    let write = KVWrite(store.main)
    let branch = ivmMain.fork()
    mutationTracker.mutationIDAssigned(ephemeralID, mutationID)
    let tx = Transaction(
      clientID: clientID, mutationID: Int(mutationID), reason: .optimistic, token: token,
      write: write, branch: branch, schema: schema)
    let result: JSONValue?
    do {
      result = try mutator(tx, args)
    } catch {
      mutationTracker.rejectMutation(ephemeralID, error)
      let outcome = Self.outcome(error)
      Self.logFailure(log, name, outcome, on: "client")
      client.resolve(outcome)
      return MutatorResult(client: client, server: server)
    }
    let pending = ReplicaStore.PendingMutation(
      id: mutationID, clientID: clientID, name: name, args: args ?? .null,
      timestamp: scheduler.monotonicNow, writes: write.writes)
    let diff = store.commitMutation(pending, write)
    database.apply([
      .addMutation(
        group: clientGroupID,
        StoredMutation(
          clientID: clientID, id: mutationID, name: name, args: pending.args,
          timestamp: pending.timestamp))
    ])
    context.processChanges(diff, rows: tx.rows)
    requestPush(now: false)
    fireWatchers(diff)
    client.resolve(.success(data: result))
    return MutatorResult(client: client, server: server)
  }

  private var token: String? { auth.flatMap { $0.isEmpty ? nil : $0 } }

  /// What a mutation's caller hears for an error (`wrapErrorFor`).
  static func outcome(_ error: any Error) -> MutationOutcome {
    if let e = error as? ConnectionError, e.isZeroError { return .zeroError(message: e.message) }
    let app = ApplicationError.wrapping(error)
    return .appError(message: app.message, details: app.details)
  }

  static func logFailure(_ log: Log, _ name: String, _ outcome: MutationOutcome, on origin: String)
  {
    switch outcome {
    case .zeroError(let message): log.error("Mutator \"\(name)\" error on \(origin): \(message)")
    case .appError(let message, _):
      log.error("Mutator \"\(name)\" app error on \(origin): \(message)")
    case .success: break
    }
  }

  /// The store watches: the mutation tracker on `m/<clientID>/`, then the
  /// query manager on `g/`. A watch hears only about diffs that touch its
  /// prefix, except for its first run.
  private func fireWatchers(_ diff: KVDiff, initial: Bool = false) {
    let mutations = diff.withPrefix(Keys.mutationsPrefix + clientID + "/")
    if initial || !mutations.isEmpty { try? mutationTracker.processMutationResponses(mutations) }
    let got = diff.withPrefix(Keys.gotQueriesPrefix)
    if initial || !got.isEmpty { try? queryManager.processGotQueries(got) }
  }

  // MARK: - Pushing

  /// `pushConnectionLoop.send(now)`.
  private func requestPush(now: Bool) {
    if repClosed { return }
    pushRequested = true
    guard !pushInFlight else { return }
    if now {
      pushTimer?.cancel()
      pushTimer = nil
      loop.microtask { [weak self] in self?.invokePush() }
    } else if pushTimer == nil {
      pushTimer = scheduler.schedule(after: 0) { [weak self] in
        self?.pushTimer = nil
        self?.invokePush()
      }
    }
  }

  /// `#invokePush`: pushes the pending mutations once connected.
  private func invokePush() {
    guard !pushInFlight, pushRequested, !repClosed, !clientGroupDisabled else { return }
    pushRequested = false
    let pending = store.pending
    guard !pending.isEmpty else { return }
    pushInFlight = true
    connectionReady.onSettle(loop) { [weak self] connected in
      guard let self else { return }
      if connected { self.pusher(pending) }
      self.pushInFlight = false
      if !connected { self.pushRequested = true }
      if self.pushRequested { self.requestPush(now: false) }
    }
  }

  /// `#pusher`: one `push` message per mutation not yet sent on this
  /// connection.
  private func pusher(_ mutations: [ReplicaStore.PendingMutation]) {
    let now = scheduler.now
    pushRequestCounter += 1
    let requestID = "\(clientID)-\(pushRequestCounter)"
    for m in mutations where m.id > (lastMutationIDsSent[m.clientID] ?? 0) {
      let timestamp = now - (scheduler.monotonicNow - m.timestamp).rounded(.toNearestOrAwayFromZero)
      send(
        .push(
          PushBody(
            clientGroupID: clientGroupID,
            mutations: [
              .custom(
                CustomMutation(
                  id: m.id, clientID: m.clientID, name: m.name, args: [m.args],
                  timestamp: timestamp))
            ],
            pushVersion: 1, timestamp: now, requestID: requestID)))
      lastMutationIDsSent[m.clientID] = m.id
    }
  }

  // MARK: - Pokes

  /// `rep.poke`: applies a merged poke to the snapshot and rebases the
  /// pending mutations on it (`handlePullResponseV1` and `maybeEndPull`).
  private func applyPoke(_ poke: MergedPoke) throws {
    let socketAtStart = socket
    let base = store.snapshot
    guard poke.baseCookie == base.cookie else {
      throw PokeError("unexpected base cookie for poke: " + JSONSerializer.stringify(poke.json))
    }
    for (cid, lmid) in poke.lastMutationIDChanges.sorted(by: {
      StringCompare.utf16Less($0.key.string, $1.key.string)
    }) {
      if let last = base.lastMutationIDs[cid], lmid < last {
        throw PokeError(
          Self.badOrderMessage(
            "\(cid) lastMutationID", JSNumber.format(lmid), JSNumber.format(last)))
      }
    }
    if compareCookies(poke.cookie, base.cookie) < 0 {
      throw PokeError(
        Self.badOrderMessage(
          "cookie", JSONSerializer.stringify(poke.cookie), JSONSerializer.stringify(base.cookie)))
    }
    if poke.cookie == base.cookie {
      // `zero.ts` trusts the got queries once `rep.poke` returns, even for a
      // poke that changes nothing.
      markGotQueriesAuthoritative(ifStillOn: socketAtStart)
      return
    }

    var snapshot = base
    snapshot.lastMutationIDs.merge(poke.lastMutationIDChanges) { $1 }
    snapshot.cookie = poke.cookie
    let patchWrite = KVWrite(snapshot.map)
    do {
      try ReplicaStore.apply(poke.patch, to: patchWrite)
    } catch {
      throw PokeError(error.description)
    }
    snapshot.map = patchWrite.map

    var touched = patchWrite.writes.union(store.pendingWrites)
    var main = snapshot.map
    var replayed: [ReplicaStore.PendingMutation] = []
    let pending = store.pending.filter {
      $0.id > (snapshot.lastMutationIDs[JSKey($0.clientID)] ?? 0)
    }
    if !pending.isEmpty {
      // The rebase runs over a fork of the views' branch moved to the new
      // snapshot.
      let branch = ivmMain.fork()
      branch.apply(KVDiff.between(store.main, snapshot.map, keys: touched))
      let rebased = replay(pending, over: main, branch: branch)
      main = rebased.main
      replayed = rebased.replayed
      touched.formUnion(rebased.touched)
    }
    let diff = store.advance(to: snapshot, main: main, pending: replayed, touched: touched)
    database.apply([
      .updateSnapshot(
        group: clientGroupID,
        changes: patchWrite.writes.compactMap { key in
          let value = snapshot.map.get(key.string)
          // After a clear, only what the poke put back.
          return patchWrite.cleared && value == nil ? nil : (key.string, value)
        },
        cleared: patchWrite.cleared, cookie: snapshot.cookie,
        lastMutationIDs: snapshot.lastMutationIDs)
    ])
    context.processChanges(diff)
    fireWatchers(diff)
    markGotQueriesAuthoritative(ifStillOn: socketAtStart)
  }

  /// The server has caught the client up, unless the connection the poke
  /// came on went away while it was applied: the disconnect already
  /// re-gated trust, and the next connection's first poke restores it.
  private func markGotQueriesAuthoritative(ifStillOn socketAtStart: SocketHandle?) {
    if socketAtStart != nil, socket === socketAtStart {
      queryManager.markGotQueriesAuthoritative()
    }
  }

  /// Runs `pending` again, in order, over `map` and on forks of `branch`
  /// (the rebase of `maybeEndPull`). A mutation that throws stays pending
  /// without writes. Returns the new main map, the mutations with what they
  /// wrote, every key they wrote, and the branch they ran on.
  private func replay(
    _ pending: [ReplicaStore.PendingMutation], over map: KVMap, branch: IVMBranch
  ) -> (main: KVMap, replayed: [ReplicaStore.PendingMutation], touched: Set<JSKey>, IVMBranch) {
    var main = map
    var branch = branch
    var touched = Set<JSKey>()
    var replayed: [ReplicaStore.PendingMutation] = []
    for var m in pending {
      let write = KVWrite(main)
      let fork = branch.fork()
      let tx = Transaction(
        clientID: m.clientID, mutationID: Int(m.id), reason: .rebase, token: token,
        write: write, branch: fork, schema: schema)
      do {
        guard let mutator = mutators[m.name] else {
          throw ApplicationError("Unknown mutator: \(m.name)")
        }
        _ = try mutator(tx, m.args)
        main = write.map
        branch = fork
        m.writes = write.writes
        touched.formUnion(write.writes)
      } catch {
        // The mutation stays pending; its writes are dropped.
        m.writes = []
      }
      replayed.append(m)
    }
    return (main, replayed, touched, branch)
  }

  /// `JSON.stringify(recentSentMessages, null, 2)`.
  static func prettyRecentMessages(_ messages: [JSONValue]) -> String {
    let entries = messages.map { m in
      let fields = ["type", "size", "snippet"].compactMap { key -> String? in
        guard let v = m[key] else { return nil }
        var k = ""
        JSONSerializer.writeString(key, into: &k)
        return "    \(k): \(JSONSerializer.stringify(v))"
      }
      return "  {\n" + fields.joined(separator: ",\n") + "\n  }"
    }
    return "[\n" + entries.joined(separator: ",\n") + "\n]"
  }

  static func badOrderMessage(_ name: String, _ received: String, _ last: String) -> String {
    "Received \(name) \(received) is < than last snapshot \(name) \(last); ignoring client view"
  }
}

/// A connection's readiness, as the pusher waits on it
/// (`#connectionReadyResolver`).
@MainActor
final class ReadyPromise {
  private var settled: Bool?
  private var waiters: [(Bool) -> Void] = []

  func resolve(_ loop: EventLoop) { settle(true, loop) }
  func reject(_ loop: EventLoop) { settle(false, loop) }

  private func settle(_ value: Bool, _ loop: EventLoop) {
    guard settled == nil else { return }
    settled = value
    let waiters = self.waiters
    self.waiters = []
    for w in waiters { loop.microtask { w(value) } }
  }

  /// Calls `waiter` in a microtask once settled.
  func onSettle(_ loop: EventLoop, _ waiter: @escaping (Bool) -> Void) {
    if let settled {
      loop.microtask { waiter(settled) }
    } else {
      waiters.append(waiter)
    }
  }
}

/// The engine's end of one socket.
@MainActor
final class SocketHandle: SocketEvents {
  weak var engine: Engine?
  var connection: (any SocketConnection)!
  private var closed = false

  init(engine: Engine) { self.engine = engine }

  func send(_ text: String) {
    guard !closed else { return }
    connection.send(text)
  }

  func close(code: Int?) {
    guard !closed else { return }
    closed = true
    connection.close(code: code)
  }

  func socketDidOpen() {}

  func socketDidReceive(_ message: SocketMessage) {
    guard !closed else { return }
    engine?.socketDidReceive(self, message)
  }

  func socketDidClose(code: Int, reason: String, wasClean: Bool) {
    guard !closed else { return }
    engine?.socketDidClose(self, code: code, reason: reason, wasClean: wasClean)
  }
}

extension MergedPoke {
  /// The poke as `JSON.stringify` shows it in errors.
  var json: JSONValue {
    [
      "baseCookie": baseCookie,
      "pullResponse": [
        "cookie": cookie, "lastMutationIDChanges": .numberRecord(lastMutationIDChanges),
      ],
    ]
  }
}

/// A random ID like the JS client's `nanoid`.
func randomID() -> String {
  let alphabet = Array("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_-")
  var g = SystemRandomNumberGenerator()
  return String((0..<21).map { _ in alphabet[Int(g.next() % UInt64(alphabet.count))] })
}

/// `Date.prototype.toISOString` of a time in milliseconds since 1970.
func isoDateString(_ ms: Double) -> String {
  let t = Int64(ms.rounded(.down))
  var days = t / 86_400_000
  var msOfDay = t % 86_400_000
  if msOfDay < 0 {
    msOfDay += 86_400_000
    days -= 1
  }
  // Howard Hinnant's civil_from_days.
  let z = days + 719_468
  let era = (z >= 0 ? z : z - 146_096) / 146_097
  let doe = z - era * 146_097
  let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365
  let doy = doe - (365 * yoe + yoe / 4 - yoe / 100)
  let mp = (5 * doy + 2) / 153
  let d = doy - (153 * mp + 2) / 5 + 1
  let m = mp < 10 ? mp + 3 : mp - 9
  let y = yoe + era * 400 + (m <= 2 ? 1 : 0)
  func pad(_ v: Int64, _ n: Int) -> String {
    let s = String(v)
    return String(repeating: "0", count: max(0, n - s.count)) + s
  }
  return
    "\(pad(y, 4))-\(pad(m, 2))-\(pad(d, 2))T\(pad(msOfDay / 3_600_000, 2)):"
    + "\(pad(msOfDay / 60_000 % 60, 2)):\(pad(msOfDay / 1000 % 60, 2)).\(pad(msOfDay % 1000, 3))Z"
}

/// The message valita gives for a message that doesn't parse
/// (`getMessage` in `packages/shared/src/valita.ts`), for the cases the
/// client can tell apart.
func valitaMessage(_ error: any Error, _ text: String) -> String {
  guard let e = error as? DecodingError else { return errorMessage(error) }
  if e.message.hasPrefix("Unknown message type") || e.message == "Expected [tag, body] tuple" {
    return "Invalid union value: \(text)"
  }
  return valitaMessage(e)
}

func valitaMessage(_ e: DecodingError) -> String {
  if e.message == "Missing property", let key = e.path.last {
    let rest = e.path.dropLast()
    return "Missing property \(key)" + (rest.isEmpty ? "" : " at \(rest.joined(separator: "."))")
  }
  return e.description
}
