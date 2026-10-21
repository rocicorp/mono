/// Builds pipelines over the sources of an IVM branch.
final class BranchDelegate: BuilderDelegate {
  let branch: IVMBranch
  init(_ branch: IVMBranch) { self.branch = branch }
  func getSource(_ tableName: String) -> (any Source)? { branch.source(tableName) }
}

/// Materializes queries over an IVM branch, registers named ones with the
/// query manager, and flushes views when a transaction commits
/// (`ZeroContext` in `packages/zero-client/src/client/context.ts`, with
/// `materializeImpl`, `runImpl`, and `preloadImpl` from
/// `packages/zql/src/query/query-delegate-base.ts`).
@MainActor
final class QueryContext {
  /// Builds a deferred pipeline; returns the release to call once every
  /// pending pipeline is built.
  typealias AttachPipeline = () throws -> () -> Void

  let branch: IVMBranch
  private let delegate: BranchDelegate
  /// Registers a named query with the server; returns its unregister.
  private let addCustomQuery: (AST, CustomQueryID, TTL, @escaping GotCallback) -> () -> Void
  private let updateCustomQuery: (CustomQueryID, TTL) -> Void
  private let batchViewUpdatesImpl: (() -> Void) -> Void
  private let microtask: (@escaping () -> Void) -> Void
  private var commitListeners: [(id: Int, listener: () -> Void)] = []
  private var nextListenerID = 0
  /// Until the replica is loaded into the sources, pipelines are built
  /// later, together (`markPipelinesReady`).
  private(set) var pipelinesReady = false
  private var pendingPipelines: [(id: Int, attach: AttachPipeline)] = []

  init(
    branch: IVMBranch,
    addCustomQuery: @escaping (AST, CustomQueryID, TTL, @escaping GotCallback) -> () -> Void,
    updateCustomQuery: @escaping (CustomQueryID, TTL) -> Void,
    batchViewUpdates: @escaping (() -> Void) -> Void,
    microtask: @escaping (@escaping () -> Void) -> Void
  ) {
    self.branch = branch
    self.delegate = BranchDelegate(branch)
    self.addCustomQuery = addCustomQuery
    self.updateCustomQuery = updateCustomQuery
    self.batchViewUpdatesImpl = batchViewUpdates
    self.microtask = microtask
  }

  /// A context for the queries a mutator runs: nothing is registered with
  /// the server, and the branch is already populated.
  static func local(_ branch: IVMBranch) -> QueryContext {
    let context = QueryContext(
      branch: branch, addCustomQuery: { _, _, _, _ in {} }, updateCustomQuery: { _, _ in },
      batchViewUpdates: { $0() }, microtask: { $0() })
    context.markPipelinesReady()
    return context
  }

  func batchViewUpdates(_ body: () -> Void) {
    var performed = false
    batchViewUpdatesImpl {
      body()
      performed = true
    }
    precondition(performed, "batchViewUpdates must call applyViewUpdates synchronously.")
  }

  func onTransactionCommit(_ listener: @escaping () -> Void) -> () -> Void {
    let id = nextListenerID
    nextListenerID += 1
    commitListeners.append((id, listener))
    return { [weak self] in self?.commitListeners.removeAll { $0.id == id } }
  }

  private func onPipelinesReady(_ attach: @escaping AttachPipeline) -> () -> Void {
    precondition(!pipelinesReady, "onPipelinesReady called while pipelines are ready")
    let id = nextListenerID
    nextListenerID += 1
    pendingPipelines.append((id, attach))
    return { [weak self] in self?.pendingPipelines.removeAll { $0.id == id } }
  }

  /// Builds and hydrates every deferred pipeline, in materialization order,
  /// as one batch of view updates.
  func markPipelinesReady() {
    if pipelinesReady { return }
    if pendingPipelines.isEmpty {
      pipelinesReady = true
      return
    }
    batchViewUpdates {
      var releases: [() -> Void] = []
      while !pendingPipelines.isEmpty {
        let (_, attach) = pendingPipelines.removeFirst()
        // A failing view has already been marked as errored; the others
        // are still built.
        if let release = try? attach() { releases.append(release) }
      }
      pipelinesReady = true
      for release in releases { release() }
      endTransaction()
    }
  }

  /// Pushes `diff` into the branch and flushes the views.
  func processChanges(_ diff: KVDiff, rows: [String: Row] = [:]) {
    batchViewUpdates {
      branch.apply(diff, rows: rows)
      endTransaction()
    }
  }

  private func endTransaction() {
    for (_, l) in commitListeners { l() }
  }

  /// The settled state of a view's query: still waiting, confirmed by the
  /// server, or failed.
  private enum Completion {
    case pending, complete, failed(ErroredQuery)
  }

  func materialize(_ query: AnyQuery, ttl: TTL = .defaultTTL) throws -> MaterializedView {
    let customQueryID = query.customQueryID
    let ast = query.ast
    let queryID =
      customQueryID.map { QueryHash.hashOfNameAndArgs($0.name, $0.args) }
      ?? QueryHash.hashOfAST(ast)
    let microtask = self.microtask
    let deferPipeline = !pipelinesReady
    var attached = !deferPipeline
    var got: Got = .no
    var queryComplete: Completion = .pending
    // `queryCompleteResolver`: the view hears about its settling one
    // microtask after it (two for a failure, as `.then().catch()` does).
    var resolved: Completion?
    var onResolved: ((Completion) -> Void)?
    let settle = { (r: Completion) in
      guard resolved == nil else { return }
      resolved = r
      onResolved?(r)
    }
    var view: ArrayView?
    var destroyed = false

    let maybeResolveComplete = {
      if attached, got == .yes, case .pending = queryComplete {
        queryComplete = .complete
        settle(.complete)
      }
    }
    let maybeMarkCached = {
      if attached, got == .cached, case .pending = queryComplete { view?.markCached() }
    }
    let gotCallback: GotCallback = { value, error in
      // Removals are deferred while mutations are pending, so a destroyed
      // view can still hear from the query manager.
      if destroyed { return }
      if let error {
        settle(.failed(error))
        queryComplete = .failed(error)
        return
      }
      got = value
      switch value {
      case .cached: maybeMarkCached()
      case .no: view?.unmarkCached()
      case .yes: maybeResolveComplete()
      }
    }

    var removeCommitObserver: (() -> Void)?
    var removePendingAttach: (() -> Void)?
    let removeAddedQuery =
      customQueryID.map { addCustomQuery(ast, $0, ttl, gotCallback) } ?? {}

    let input: any Input
    let deferred: DeferredInput?
    do {
      if deferPipeline {
        let delegate = self.delegate
        let build = { () throws -> any Input in try buildPipeline(ast, delegate, queryID: queryID) }
        // A probe gets the schema, and makes a query that cannot be built
        // throw now rather than later.
        let probe = try build()
        let schema = probe.getSchema()
        probe.destroy()
        deferred = DeferredInput(schema: schema, build: build)
        input = deferred!
      } else {
        deferred = nil
        input = try buildPipeline(ast, delegate, queryID: queryID)
      }
    } catch {
      removeAddedQuery()
      throw error
    }

    var created: ArrayView!
    batchViewUpdates {
      let v: ArrayView
      switch queryComplete {
      case .complete: v = ArrayView(input, format: query.format, error: nil, complete: true)
      case .failed(let e): v = ArrayView(input, format: query.format, error: e, complete: false)
      case .pending:
        v = ArrayView(input, format: query.format, error: nil, complete: false)
        onResolved = { [weak v] (r: Completion) in
          switch r {
          case .complete: microtask { v?.setResultType(.complete) }
          case .failed(let e): microtask { microtask { v?.setResultType(.error, error: e) } }
          case .pending: break
          }
        }
        if let resolved { onResolved!(resolved) }
      }
      removeCommitObserver = onTransactionCommit { [weak v] in v?.flush() }
      created = v
    }
    created.onDestroy = {
      destroyed = true
      removePendingAttach?()
      removePendingAttach = nil
      input.destroy()
      removeCommitObserver?()
      removeAddedQuery()
    }

    if let deferred {
      removePendingAttach = onPipelinesReady { [weak created] in
        removePendingAttach = nil
        if deferred.destroyed { return {} }
        do {
          created?.holdData()
          try deferred.attach()
        } catch {
          let e = ErroredQuery.app(
            id: queryID, name: customQueryID?.name ?? "", message: errorMessage(error),
            details: nil)
          queryComplete = .failed(e)
          settle(.failed(e))
          throw error
        }
        return {
          if destroyed { return }
          attached = true
          created?.releaseData()
          maybeResolveComplete()
          maybeMarkCached()
        }
      }
    }

    view = created
    maybeMarkCached()
    let updateTTL: (TTL) -> Void =
      customQueryID.map { id in { [weak self] ttl in self?.updateCustomQuery(id, ttl) } } ?? { _ in
      }
    return MaterializedView(created, updateTTL: updateTTL)
  }

  enum RunType: String, Sendable {
    case unknown, cached, complete
  }

  /// `runImpl`: the rows now, or once the server has confirmed them.
  func run(
    _ query: AnyQuery, type: RunType = .unknown, ttl: TTL? = nil
  ) throws -> Deferred<
    ViewValue
  > {
    let v = try materialize(query, ttl: ttl ?? .defaultTTL)
    let result = Deferred<ViewValue>()
    if type == .unknown {
      // While the pipelines are not ready the view is deferred and this is
      // empty: `unknown` does not wait for local data either.
      let data = v.data
      v.destroy()
      result.resolve(data)
      return result
    }
    _ = v.addListener { data, resultType, error in
      if resultType == .complete || (type == .cached && resultType == .cached) {
        v.destroy()
        result.resolve(data)
      } else if resultType == .error {
        v.destroy()
        result.reject(QueryError(error, data: data))
      }
    }
    return result
  }

  /// The rows of `query` now, for a query run inside a mutator.
  func snapshot(_ query: AnyQuery) throws -> ViewValue {
    let v = try materialize(query)
    let data = v.data
    v.destroy()
    return data
  }

  /// `preloadImpl`: keeps a query's rows synced without a view.
  func preload(_ query: AnyQuery, ttl: TTL = .defaultPreloadTTL) -> Preload {
    let cached = Deferred<Void>()
    let complete = Deferred<Void>()
    guard let id = query.customQueryID else {
      return Preload(cleanup: {}, cached: cached, complete: complete)
    }
    let cleanup = addCustomQuery(query.ast, id, ttl) { got, error in
      if let error {
        cached.reject(QueryError(error))
        complete.reject(QueryError(error))
        return
      }
      if got == .yes {
        cached.resolve(())
        complete.resolve(())
      } else if got == .cached {
        cached.resolve(())
      }
    }
    return Preload(cleanup: cleanup, cached: cached, complete: complete)
  }
}

/// What `preload` returns.
@MainActor
public struct Preload {
  /// Stops syncing the query (after its TTL).
  public let cleanup: () -> Void
  /// Settles once the store holds a server-confirmed result, from this
  /// connection or an earlier one.
  public let cached: Deferred<Void>
  /// Settles once the server has confirmed the result on this connection.
  public let complete: Deferred<Void>
}

/// A query the server could not run.
public struct QueryError: Error, Sendable, CustomStringConvertible {
  public let error: ErroredQuery?
  /// What the view held when the query failed. JS rejects `run` with it.
  let data: ViewValue?

  package init(_ error: ErroredQuery?, data: ViewValue? = nil) {
    self.error = error
    self.data = data
  }

  public var description: String {
    switch error {
    case .app(_, _, let message, _)?: message ?? "Query failed"
    case .parse(_, _, let message, _)?: message
    case nil: "Query failed"
    }
  }
}

/// A view of a query that stays current as data changes (`TypedView`).
///
/// Unlike in JS, dropping the last reference to a view destroys it. When
/// `Zero` replaces its client (see `ZeroOptions.onClientStateNotFound`),
/// the view moves to the new client and keeps its identity and listeners.
@MainActor
public final class MaterializedView {
  public typealias Listener = (ViewValue, ResultType, ErroredQuery?) -> Void

  private(set) var view: ArrayView
  private var updateTTLImpl: (TTL) -> Void
  private var removeForward: () -> Void = {}
  private var destroyed = false
  private var listeners: [(id: Int, listener: Listener)] = []
  private var nextListenerID = 0
  /// What a new client materializes again: set by `Zero`.
  var query: AnyQuery?
  var ttl: TTL = .defaultTTL

  init(_ view: ArrayView, updateTTL: @escaping (TTL) -> Void) {
    self.view = view
    self.updateTTLImpl = updateTTL
    forward()
  }

  isolated deinit {
    destroy()
  }

  public var data: ViewValue { view.data }
  public var resultType: ResultType { view.listenerResultType.0 }
  public var error: ErroredQuery? { view.listenerResultType.1 }
  public var isDestroyed: Bool { destroyed }

  /// The primary key columns of the rows, which make their `RowID`s.
  public var primaryKey: [String] { view.input.getSchema().primaryKey }

  /// Calls `listener` now and whenever the data or result type change.
  @discardableResult
  public func addListener(_ listener: @escaping Listener) -> () -> Void {
    let id = nextListenerID
    nextListenerID += 1
    listeners.append((id, listener))
    let (type, error) = view.listenerResultType
    listener(view.data, type, error)
    return { [weak self] in self?.listeners.removeAll { $0.id == id } }
  }

  /// Calls `listener` now and whenever the data or result type change,
  /// with how the rows changed since its previous call.
  @discardableResult
  public func addSnapshotListener(_ listener: @escaping (ViewSnapshot) -> Void) -> () -> Void {
    var differ = RowDiffer(primaryKey: primaryKey)
    return addListener { data, type, error in listener(differ.next(data, type, error)) }
  }

  /// The view's snapshots: the current one, then one per change, until the
  /// iterating task is cancelled. The stream keeps the view alive.
  public var snapshots: AsyncStream<ViewSnapshot> {
    let (stream, continuation) = AsyncStream<ViewSnapshot>.makeStream()
    let subscription = SnapshotSubscription(self)
    subscription.remove = addSnapshotListener { continuation.yield($0) }
    continuation.onTermination = { _ in
      Task { @MainActor in subscription.cancel() }
    }
    return stream
  }

  private func forward() {
    removeForward = view.addListener { [weak self] data, type in
      guard let self else { return }
      let error = self.view.listenerResultType.1
      for (_, listener) in self.listeners { listener(data, type, error) }
    }
  }

  /// Takes over the pipeline of `other`, a view of the same query on a new
  /// client, and tells the listeners about its data.
  func rebind(to other: MaterializedView) {
    guard !destroyed else { return }
    removeForward()
    view.destroy()
    other.removeForward()
    other.destroyed = true
    view = other.view
    updateTTLImpl = other.updateTTLImpl
    forward()
  }

  /// Stops updating the view and releases its query, which the server
  /// keeps syncing for the query's TTL.
  public func destroy() {
    guard !destroyed else { return }
    destroyed = true
    removeForward()
    listeners = []
    view.destroy()
  }

  /// Raises the query's TTL; a lower one is ignored.
  public func updateTTL(_ ttl: TTL) {
    self.ttl = ttl
    updateTTLImpl(ttl)
  }
}

/// Keeps a view alive while its snapshot stream is iterated.
@MainActor
private final class SnapshotSubscription {
  let view: MaterializedView
  var remove: (() -> Void)?

  init(_ view: MaterializedView) { self.view = view }

  func cancel() {
    remove?()
    remove = nil
  }
}
