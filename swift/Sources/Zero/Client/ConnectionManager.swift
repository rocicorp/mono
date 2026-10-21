/// The connection status state machine
/// (`packages/zero-client/src/client/connection-manager.ts`).
///
/// Subscribers hear about every transition synchronously. `onNextStateChange`
/// waiters hear about the next one in a microtask, as the JS run loop's
/// `waitForStateChange()` promise does.
@MainActor
final class ConnectionManager {
  enum State {
    case initializing
    case disconnected(reason: ConnectionError)
    case connecting(attempt: Int, disconnectAt: Double, reason: ConnectionError?)
    case connected
    case needsAuth(reason: ConnectionError)
    case error(reason: ConnectionError)
    case closed(reason: ConnectionError)

    var status: ConnectionStatus {
      switch self {
      case .initializing: .initializing
      case .disconnected: .disconnected
      case .connecting: .connecting
      case .connected: .connected
      case .needsAuth: .needsAuth
      case .error: .error
      case .closed: .closed
      }
    }

    var reason: ConnectionError? {
      switch self {
      case .initializing, .connected: nil
      case .connecting(_, _, let reason): reason
      case .disconnected(let r), .needsAuth(let r), .error(let r), .closed(let r): r
      }
    }

    /// needs-auth and error: the run loop pauses until `connect()`.
    var isTerminal: Bool {
      switch self {
      case .needsAuth, .error: true
      default: false
      }
    }
  }

  private(set) var state: State = .initializing
  private let disconnectTimeout: Double
  private let timeoutCheckInterval: Double
  private let scheduler: any Scheduler
  private let microtask: (@escaping () -> Void) -> Void

  private var subscribers: [(id: Int, listener: (State) -> Void)] = []
  private var nextSubscriberID = 0
  private var stateWaiters: [(State) -> Void] = []
  private var connectRequested = false
  private var connectRequestWaiters: [() -> Void] = []

  private var connectingStartedAt: Double?
  private var timeoutTimer: (any ScheduledTimer)?
  private var lastTimeoutCheckAt: Double

  init(
    disconnectTimeout: Double, timeoutCheckInterval: Double = 1000, scheduler: any Scheduler,
    microtask: @escaping (@escaping () -> Void) -> Void
  ) {
    self.disconnectTimeout = disconnectTimeout
    self.timeoutCheckInterval = timeoutCheckInterval
    self.scheduler = scheduler
    self.microtask = microtask
    lastTimeoutCheckAt = scheduler.now
  }

  func `is`(_ status: ConnectionStatus) -> Bool { state.status == status }

  var isInTerminalState: Bool { state.isTerminal }

  var shouldContinueRunLoop: Bool { !self.is(.closed) }

  @discardableResult
  func subscribe(_ listener: @escaping (State) -> Void) -> () -> Void {
    let id = nextSubscriberID
    nextSubscriberID += 1
    subscribers.append((id, listener))
    return { [weak self] in self?.subscribers.removeAll { $0.id == id } }
  }

  /// Calls `waiter` in a microtask after the next transition.
  func onNextStateChange(_ waiter: @escaping (State) -> Void) {
    stateWaiters.append(waiter)
  }

  func requestConnect() {
    guard !connectRequested else { return }
    connectRequested = true
    let waiters = connectRequestWaiters
    connectRequestWaiters = []
    for w in waiters { microtask(w) }
  }

  /// Calls `waiter` in a microtask once `connect()` is requested (right
  /// away if it already was).
  func onConnectRequest(_ waiter: @escaping () -> Void) {
    if connectRequested {
      microtask(waiter)
    } else {
      connectRequestWaiters.append(waiter)
    }
  }

  private func consumeConnectRequest() -> Bool {
    guard connectRequested else { return false }
    connectRequested = false
    return true
  }

  /// Consumes a pending connect request and resumes connecting.
  @discardableResult
  func resumeFromConnectRequest() -> Bool {
    guard isInTerminalState, consumeConnectRequest() else { return false }
    connecting()
    return true
  }

  /// From initializing to connecting, once the local store is loaded. This is
  /// where the connecting window starts.
  func initialized() {
    guard case .initializing = state else { return }
    let now = scheduler.now
    connectingStartedAt = now
    state = .connecting(attempt: 0, disconnectAt: now + disconnectTimeout, reason: nil)
    maybeStartTimeoutInterval()
    publish()
  }

  func connecting(_ reason: ConnectionError? = nil) {
    if case .closed = state { return }
    var isHiddenDisconnect = false
    if case .disconnected(let r) = state {
      guard r.clientKind == .hidden else { return }
      isHiddenDisconnect = true
    }
    let now = scheduler.now
    if isHiddenDisconnect { connectingStartedAt = nil }
    if case .connecting(let attempt, let disconnectAt, _) = state {
      state = .connecting(attempt: attempt + 1, disconnectAt: disconnectAt, reason: reason)
      maybeStartTimeoutInterval()
      publish()
      return
    }
    if connectingStartedAt == nil { connectingStartedAt = now }
    state = .connecting(
      attempt: 1, disconnectAt: connectingStartedAt! + disconnectTimeout, reason: reason)
    maybeStartTimeoutInterval()
    publish()
  }

  func connected() {
    switch state {
    case .closed, .connected: return
    default: break
    }
    connectingStartedAt = nil
    maybeStopTimeoutInterval()
    state = .connected
    publish()
  }

  func disconnected(_ reason: ConnectionError) {
    switch state {
    case .closed, .disconnected: return
    case .connected: connectingStartedAt = nil
    default: break
    }
    maybeStopTimeoutInterval()
    state = .disconnected(reason: reason)
    publish()
  }

  func needsAuth(_ reason: ConnectionError) {
    switch state {
    case .closed, .needsAuth: return
    default: break
    }
    connectingStartedAt = nil
    maybeStopTimeoutInterval()
    state = .needsAuth(reason: reason)
    publish()
  }

  func error(_ reason: ConnectionError) {
    switch state {
    case .closed, .error: return
    default: break
    }
    connectingStartedAt = nil
    maybeStopTimeoutInterval()
    state = .error(reason: reason)
    publish()
  }

  func closed() {
    if case .closed = state { return }
    connectingStartedAt = nil
    maybeStopTimeoutInterval()
    state = .closed(
      reason: .client(
        .clientClosed, message: "Zero was explicitly closed by calling zero.close()"))
    publish()
    subscribers = []
    resolveStateWaiters()
  }

  private func publish() {
    let state = self.state
    for (_, listener) in subscribers { listener(state) }
    resolveStateWaiters()
  }

  private func resolveStateWaiters() {
    let state = self.state
    let waiters = stateWaiters
    stateWaiters = []
    for w in waiters { microtask { w(state) } }
  }

  /// Pushes the disconnect deadline forward by however long timers were not
  /// running (a suspended app, a sleeping laptop).
  private func creditFrozenTime() {
    let now = scheduler.now
    let elapsed = now - lastTimeoutCheckAt
    lastTimeoutCheckAt = now
    if elapsed <= 2 * timeoutCheckInterval { return }
    if let started = connectingStartedAt { connectingStartedAt = started + elapsed }
    if case .connecting(let attempt, let disconnectAt, let reason) = state {
      state = .connecting(attempt: attempt, disconnectAt: disconnectAt + elapsed, reason: reason)
    }
  }

  private func checkTimeout() {
    guard case .connecting(_, let disconnectAt, _) = state else { return }
    if scheduler.now >= disconnectAt {
      disconnected(
        .client(
          .offline,
          message:
            "Zero was unable to connect for \(Int((disconnectTimeout / 1000).rounded(.down))) seconds and was disconnected"
        ))
    }
  }

  private func maybeStartTimeoutInterval() {
    guard timeoutTimer == nil else { return }
    lastTimeoutCheckAt = scheduler.now
    timeoutTimer = scheduler.scheduleRepeating(every: timeoutCheckInterval) { [weak self] in
      guard let self else { return }
      self.creditFrozenTime()
      self.checkTimeout()
    }
  }

  private func maybeStopTimeoutInterval() {
    timeoutTimer?.cancel()
    timeoutTimer = nil
  }
}

/// The status names (`packages/zero-client/src/client/connection-status-enum.ts`).
public enum ConnectionStatus: String, Sendable, Hashable {
  case initializing
  case disconnected
  case connecting
  case connected
  case needsAuth = "needs-auth"
  case error
  case closed
}

/// The connection state an app sees (`ConnectionState` in
/// `packages/zero-client/src/client/connection.ts`).
public enum ConnectionState: Sendable, Hashable {
  case initializing
  case disconnected(reason: String)
  case connecting(reason: String?)
  case connected
  case needsAuth(NeedsAuthReason)
  case error(reason: String)
  case closed(reason: String)

  public var status: ConnectionStatus {
    switch self {
    case .initializing: .initializing
    case .disconnected: .disconnected
    case .connecting: .connecting
    case .connected: .connected
    case .needsAuth: .needsAuth
    case .error: .error
    case .closed: .closed
    }
  }

  init(_ state: ConnectionManager.State) {
    switch state {
    case .initializing: self = .initializing
    case .closed(let r): self = .closed(reason: r.message)
    case .connected: self = .connected
    case .connecting(_, _, let r):
      self = .connecting(reason: r.flatMap { $0.message.isEmpty ? nil : $0.message })
    case .disconnected(let r): self = .disconnected(reason: r.message)
    case .error(let r): self = .error(reason: r.message)
    case .needsAuth(let r): self = .needsAuth(NeedsAuthReason(r))
    }
  }

  /// The JSON form of the JS `ConnectionState`.
  package var json: JSONValue {
    var o: [String: JSONValue] = ["name": .string(status.rawValue)]
    switch self {
    case .initializing, .connected: break
    case .connecting(let reason): if let reason { o["reason"] = .string(reason) }
    case .disconnected(let r), .error(let r), .closed(let r): o["reason"] = .string(r)
    case .needsAuth(let r): o["reason"] = r.json
    }
    return .object(o)
  }
}

/// Why the client needs new credentials.
public enum NeedsAuthReason: Sendable, Hashable {
  /// The API server answered a push with 401 or 403.
  case mutate(status: Int, body: String?)
  /// The API server answered a query transform with 401 or 403.
  case query(status: Int, body: String?)
  /// zero-cache rejected the credentials.
  case zeroCache(reason: String)

  init(_ error: ConnectionError) {
    if case .server(let body) = error {
      switch body {
      case .pushFailed(let b):
        if case .http(let status, let preview) = b.cause {
          self = .mutate(status: Int(status), body: preview.flatMap { $0.isEmpty ? nil : $0 })
          return
        }
      case .transformFailed(let b):
        if case .http(let status, let preview) = b.cause {
          self = .query(status: Int(status), body: preview.flatMap { $0.isEmpty ? nil : $0 })
          return
        }
      default: break
      }
    }
    self = .zeroCache(reason: error.message)
  }

  package var json: JSONValue {
    switch self {
    case .mutate(let status, let body), .query(let status, let body):
      var o: [String: JSONValue] = [
        "type": .string(self.isMutate ? "mutate" : "query"), "status": .number(Double(status)),
      ]
      if let body { o["body"] = .string(body) }
      return .object(o)
    case .zeroCache(let reason):
      return ["type": "zero-cache", "reason": .string(reason)]
    }
  }

  private var isMutate: Bool {
    if case .mutate = self { return true }
    return false
  }
}
