/// What a query's got callback hears: whether the server has confirmed the
/// query's rows on this connection (`true`), on an earlier one (`cached`),
/// or not (`false`).
enum Got: Equatable {
  case no, cached, yes
}

typealias GotCallback = (Got, ErroredQuery?) -> Void

/// A dictionary that iterates in insertion order, like a JS `Map`.
struct OrderedMap<Key: Hashable, Value> {
  private(set) var keys: [Key] = []
  private var values: [Key: Value] = [:]

  subscript(key: Key) -> Value? {
    get { values[key] }
    set {
      if let newValue {
        if values.updateValue(newValue, forKey: key) == nil { keys.append(key) }
      } else if values.removeValue(forKey: key) != nil {
        keys.removeAll { $0 == key }
      }
    }
  }

  var count: Int { keys.count }
  var isEmpty: Bool { keys.isEmpty }
  var entries: [(key: Key, value: Value)] { keys.map { ($0, values[$0]!) } }
}

/// Tracks the queries the client wants from the server, tells the server
/// about changes in batches, and reports which ones the server has sent
/// (`packages/zero-client/src/client/query-manager.ts`).
@MainActor
final class QueryManager {
  final class Entry {
    let normalized: AST
    let name: String?
    let args: [JSONValue]?
    var count: Int
    var gotCallbacks: [(id: Int, callback: GotCallback)]
    var ttl: Double

    init(
      normalized: AST, name: String?, args: [JSONValue]?, gotCallbacks: [(Int, GotCallback)],
      ttl: Double
    ) {
      self.normalized = normalized
      self.name = name
      self.args = args
      self.count = 1
      self.gotCallbacks = gotCallbacks
      self.ttl = ttl
    }
  }

  private let clientID: String
  private let clientToServer: NameMapper
  private let send: (ChangeDesiredQueriesBody) -> Void
  private let onFatalError: (ConnectionError) -> Void
  private let mutationTracker: MutationTracker
  private let scheduler: any Scheduler
  private let queryChangeThrottleMs: Double
  private let recentQueriesMaxSize: Int

  private var queries = OrderedMap<JSKey, Entry>()
  private var recentQueries = OrderedMap<JSKey, Void>()
  private var gotQueries = Set<JSKey>()
  private var gotQueriesAuthoritative = false
  private var cachedQueries = Set<JSKey>()
  private var awaitingPersistedGotDiff = true
  private var pendingQueryChanges: UpQueriesPatch = []
  private var pendingRemovals: [() -> Void] = []
  private var batchTimer: (any ScheduledTimer)?
  private var closedError: ConnectionError?
  private var nextCallbackID = 0

  init(
    clientID: String, schema: ZeroSchema, mutationTracker: MutationTracker,
    scheduler: any Scheduler, queryChangeThrottleMs: Double, recentQueriesMaxSize: Int,
    send: @escaping (ChangeDesiredQueriesBody) -> Void,
    onFatalError: @escaping (ConnectionError) -> Void
  ) {
    self.clientID = clientID
    self.clientToServer = NameMapper.clientToServer(schema)
    self.mutationTracker = mutationTracker
    self.scheduler = scheduler
    self.queryChangeThrottleMs = queryChangeThrottleMs
    self.recentQueriesMaxSize = recentQueriesMaxSize
    self.send = send
    self.onFatalError = onFatalError
    mutationTracker.onAllMutationsApplied { [weak self] in
      guard let self, !self.pendingRemovals.isEmpty else { return }
      let removals = self.pendingRemovals
      self.pendingRemovals = []
      for r in removals { r() }
    }
  }

  /// The watch of `g/`. The first call is the got set already in the store.
  func processGotQueries(_ diff: KVDiff) throws {
    let persisted = awaitingPersistedGotDiff
    awaitingPersistedGotDiff = false
    var thrown: (any Error)?
    for op in diff {
      let hash = JSKey(String(op.key.utf8.dropFirst(Keys.gotQueriesPrefix.utf8.count))!)
      switch op {
      case .add:
        gotQueries.insert(hash)
        if gotQueriesAuthoritative {
          thrown = thrown ?? fireGotCallbacks(hash, .yes)
        } else if persisted {
          cachedQueries.insert(hash)
          thrown = thrown ?? fireGotCallbacks(hash, .cached)
        }
      case .del:
        gotQueries.remove(hash)
        cachedQueries.remove(hash)
        thrown = thrown ?? fireGotCallbacks(hash, .no)
      case .change:
        break
      }
    }
    if let thrown { throw thrown }
  }

  private func fireGotCallbacks(_ hash: JSKey, _ got: Got) -> (any Error)? {
    guard let entry = queries[hash] else { return nil }
    for (_, cb) in entry.gotCallbacks { cb(got, nil) }
    return nil
  }

  /// Trusts the got set once the first poke after a connect is applied.
  func markGotQueriesAuthoritative() {
    if gotQueriesAuthoritative { return }
    gotQueriesAuthoritative = true
    cachedQueries.removeAll()
    awaitingPersistedGotDiff = false
    for hash in queries.keys where gotQueries.contains(hash) {
      _ = fireGotCallbacks(hash, .yes)
    }
  }

  /// On disconnect: the next connection must confirm the got set again.
  func clearGotQueriesAuthoritative() {
    guard gotQueriesAuthoritative else { return }
    gotQueriesAuthoritative = false
    cachedQueries.formUnion(gotQueries)
  }

  /// The changes to send so the server's desired queries for this client
  /// (`d/<clientID>/` in the store) become ours, less what `lastPatch`
  /// already sent.
  func queriesPatch(
    store: ReplicaStore, lastPatch: OrderedMap<JSKey, UpQueriesPatchOp>? = nil
  ) -> OrderedMap<JSKey, UpQueriesPatchOp> {
    let prefix = Keys.desiredQueriesPrefix + clientID + "/"
    var existing: [JSKey] = []
    var existingSet = Set<JSKey>()
    for e in store.main.scan(prefix: prefix) {
      let hash = JSKey(String(e.key.utf8.dropFirst(prefix.utf8.count))!)
      if existingSet.insert(hash).inserted { existing.append(hash) }
    }
    var patch = OrderedMap<JSKey, UpQueriesPatchOp>()
    for hash in existing where queries[hash] == nil {
      patch[hash] = .del(hash: hash.string)
    }
    for (hash, entry) in queries.entries where !existingSet.contains(hash) {
      patch[hash] = put(hash.string, entry, ttl: entry.ttl)
    }
    if let lastPatch {
      for (hash, op) in lastPatch.entries {
        if case .put = op, patch[hash] == nil { patch[hash] = .del(hash: hash.string) }
      }
      for (hash, op) in patch.entries {
        if let last = lastPatch[hash], Self.sameOp(last, op) { patch[hash] = nil }
      }
    }
    return patch
  }

  private static func sameOp(_ a: UpQueriesPatchOp, _ b: UpQueriesPatchOp) -> Bool {
    switch (a, b) {
    case (.put, .put), (.del, .del), (.clear, .clear): true
    default: false
    }
  }

  private func put(_ hash: String, _ entry: Entry, ttl: Double) -> UpQueriesPatchOp {
    .put(
      UpQueriesPut(
        hash: hash, ttl: ttl, ast: entry.name == nil ? entry.normalized : nil, name: entry.name,
        args: entry.args))
  }

  func handleTransformErrors(_ errors: [ErroredQuery]) {
    for error in errors {
      guard let entry = queries[JSKey(error.id)] else { continue }
      for (_, cb) in entry.gotCallbacks { cb(.no, error) }
    }
  }

  func handleClosed(_ reason: ConnectionError) {
    guard closedError == nil else { return }
    closedError = reason
    for (hash, entry) in queries.entries {
      let error = Self.closedError(id: hash.string, name: entry.name, reason)
      for (_, cb) in entry.gotCallbacks { cb(.no, error) }
    }
  }

  /// The error of a query that waited for the server when the client
  /// closed (or, in Swift only, that waits for it once closed).
  static func closedError(id: String, name: String?, _ reason: ConnectionError) -> ErroredQuery {
    .app(
      id: id, name: name ?? "legacy", message: reason.message,
      details: ["kind": .string(reason.kindName)])
  }

  /// Registers a named query. Returns the function that unregisters it.
  func addCustom(
    _ ast: AST, name: String, args: [JSONValue], ttl: TTL, gotCallback: GotCallback?
  ) -> () -> Void {
    let normalized = ast.normalized()
    let hash = QueryHash.hashOfNameAndArgs(name, args)
    return add(JSKey(hash), normalized, name, args, ttl, gotCallback)
  }

  private func add(
    _ hash: JSKey, _ normalized: AST, _ name: String, _ args: [JSONValue], _ ttl: TTL,
    _ gotCallback: GotCallback?
  ) -> () -> Void {
    let ttl = ttl.clamped
    var callbackID: Int?
    if gotCallback != nil {
      nextCallbackID += 1
      callbackID = nextCallbackID
    }
    recentQueries[hash] = nil
    let entry: Entry
    if let existing = queries[hash] {
      entry = existing
      entry.count += 1
      updateEntry(entry, hash, ttl)
      if let gotCallback { entry.gotCallbacks.append((callbackID!, gotCallback)) }
    } else {
      let mapped = (try? clientToServer.map(normalized)) ?? normalized
      entry = Entry(
        normalized: mapped, name: name, args: args,
        gotCallbacks: gotCallback.map { [(callbackID!, $0)] } ?? [], ttl: ttl)
      queries[hash] = entry
      queueQueryChange(put(hash.string, entry, ttl: ttl))
    }
    if let gotCallback {
      let got: Got =
        gotQueriesAuthoritative
        ? (gotQueries.contains(hash) ? .yes : .no)
        : (cachedQueries.contains(hash) ? .cached : .no)
      gotCallback(got, nil)
    }
    var removed = false
    return { [weak self] in
      guard let self, !removed else { return }
      removed = true
      // Queries can't be removed while mutations are pending: a rebase
      // could need data they keep in scope.
      if self.mutationTracker.size > 0 {
        self.pendingRemovals.append { [weak self] in self?.remove(entry, hash, callbackID) }
        return
      }
      self.remove(entry, hash, callbackID)
    }
  }

  func updateCustom(name: String, args: [JSONValue], ttl: TTL) {
    let hash = JSKey(QueryHash.hashOfNameAndArgs(name, args))
    guard let entry = queries[hash] else { return }
    updateEntry(entry, hash, ttl.clamped)
  }

  private func updateEntry(_ entry: Entry, _ hash: JSKey, _ ttl: Double) {
    if TTL.compare(.milliseconds(ttl), .milliseconds(entry.ttl)) > 0 {
      entry.ttl = ttl
      queueQueryChange(put(hash.string, entry, ttl: ttl))
    }
  }

  private func queueQueryChange(_ op: UpQueriesPatchOp) {
    pendingQueryChanges.append(op)
    if batchTimer == nil {
      batchTimer = scheduler.schedule(after: queryChangeThrottleMs) { [weak self] in
        self?.flushBatch()
      }
    }
  }

  func flushBatch() {
    batchTimer?.cancel()
    batchTimer = nil
    guard !pendingQueryChanges.isEmpty else { return }
    let changes = pendingQueryChanges
    pendingQueryChanges = []
    send(ChangeDesiredQueriesBody(desiredQueriesPatch: changes))
  }

  private func remove(_ entry: Entry, _ hash: JSKey, _ callbackID: Int?) {
    if let callbackID { entry.gotCallbacks.removeAll { $0.id == callbackID } }
    entry.count -= 1
    guard entry.count == 0 else { return }
    recentQueries[hash] = ()
    if recentQueries.count > recentQueriesMaxSize, let lru = recentQueries.keys.first {
      queries[lru] = nil
      recentQueries[lru] = nil
      queueQueryChange(.del(hash: lru.string))
    }
  }
}
