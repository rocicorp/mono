import Observation
import Zero

/// A list that pages its rows in from Zero queries as it scrolls, keeping
/// what's on screen in place (`useZeroVirtualizer` in
/// `@rocicorp/zero-virtual`).
///
/// It runs at most three queries: a page of rows around where the list is
/// scrolled, or, opened at a permalink, the row and a page either side of
/// it. Rows far from the screen are dropped.
///
/// ```swift
/// let list = ZeroVirtualizer(
///   zero: zero, listContext: filter, estimatedRowHeight: 56,
///   rowKey: \.id, toStartRow: { IssueStart(id: $0.id, modified: $0.modified) },
///   pageQuery: { page, filter in Queries.issuePage(filter, page) },
///   rowQuery: { row, filter in Queries.issueByID(filter, row.id) })
/// ```
///
/// A scroll view adapter (`VirtualList`, `VirtualTableAdapter`) attaches
/// itself and renders `snapshot`.
@Observable
@MainActor
public final class ZeroVirtualizer<
  Context: Sendable, Row: ZeroRow, Key: Hashable, StartRow: Equatable & Sendable,
  ListContext: Equatable & Sendable
> {
  /// A page of the list `ListContext` names.
  public typealias PageQuery = (VirtualPageRequest<StartRow>, ListContext) -> QueryRequest<
    Row, [Row], Context
  >
  /// The row an id names, if it's in the list `ListContext` names.
  public typealias RowQuery = (VirtualRowRequest, ListContext) -> QueryRequest<Row, Row?, Context>
  public typealias ScrollState = VirtualScrollState<StartRow, ListContext>

  /// What to render.
  public private(set) var snapshot = VirtualSnapshot<Row, Key>()
  /// A query couldn't be run, or its rows read.
  public private(set) var error: (any Error)?

  @ObservationIgnored let core: VirtualizerCore<ListContext, Row, Key, StartRow>
  @ObservationIgnored private let zero: Zero<Context>
  @ObservationIgnored private var pageQuery: PageQuery
  @ObservationIgnored private var rowQuery: RowQuery
  @ObservationIgnored private let ttl: TTL
  @ObservationIgnored private var single = Slot<Row?>()
  @ObservationIgnored private var main = Slot<[Row]>()
  @ObservationIgnored private var after = Slot<[Row]>()
  @ObservationIgnored private var syncing = false
  @ObservationIgnored private var dirty = false
  /// Called with each snapshot published: the adapter re-renders and calls
  /// `afterUpdate` once it's laid out, as a React render does, even when
  /// the rows are the same ones.
  @ObservationIgnored package var onSnapshot: (() -> Void)?

  /// - Parameters:
  ///   - listContext: What the list shows, such as its filter and sort. A
  ///     new value starts the list over at its top (or `permalinkID`).
  ///   - estimatedRowHeight: The height that stands in for rows not loaded.
  ///   - rowKey: The identity of a row in the views.
  ///   - toStartRow: What a page query needs of a row to start from it.
  ///   - pageQuery: A page: `limit` rows after `start` (forward), or before
  ///     it closest first (backward), in the list's order.
  ///   - rowQuery: The row an id names, if it's in the list.
  ///   - permalinkID: A row to open the list at.
  ///   - scrollState: Where the list was, from `onScrollStateChange`.
  public init(
    zero: Zero<Context>, listContext: ListContext, estimatedRowHeight: Double,
    rowKey: @escaping (Row) -> Key, toStartRow: @escaping (Row) -> StartRow,
    pageQuery: @escaping PageQuery, rowQuery: @escaping RowQuery, permalinkID: String? = nil,
    scrollState: ScrollState? = nil, minPageSize: Int? = nil, settleTime: Duration = .seconds(2),
    stickToBottom: Bool = false, ttl: TTL = .defaultTTL, scheduler: (any Scheduler)? = nil
  ) {
    var options = VirtualizerOptions<ListContext, Row, Key, StartRow>(
      estimatedRowHeight: estimatedRowHeight, rowKey: rowKey, toStartRow: toStartRow,
      listContext: listContext)
    options.permalinkID = permalinkID
    options.scrollState = scrollState
    options.minPageSize = minPageSize
    options.settleTime =
      Double(settleTime.components.seconds) * 1000
      + Double(settleTime.components.attoseconds) / 1e15
    options.stickToBottom = stickToBottom
    self.zero = zero
    self.pageQuery = pageQuery
    self.rowQuery = rowQuery
    self.ttl = ttl
    core = VirtualizerCore(options, scheduler: scheduler ?? SystemScheduler())
    _ = core.subscribe { [weak self] in self?.sync() }
    sync()
  }

  isolated deinit {
    core.detach()
    single.close()
    main.close()
    after.close()
  }

  // MARK: Options

  /// What the list shows. A new value starts it over.
  public var listContext: ListContext {
    get { core.options.listContext }
    set { update { $0.listContext = newValue } }
  }

  /// A row the list is opened at. A new value scrolls to it.
  public var permalinkID: String? {
    get { core.options.permalinkID }
    set { update { $0.permalinkID = newValue } }
  }

  /// How the list keeps what's on screen in place. Adapters set it.
  public var anchoring: VirtualAnchoring {
    get { core.options.anchoring }
    set { update { $0.anchoring = newValue } }
  }

  public var estimatedRowHeight: Double {
    get { core.options.estimatedRowHeight }
    set { update { $0.estimatedRowHeight = newValue } }
  }

  /// Follow the bottom while the list is parked there.
  public var stickToBottom: Bool {
    get { core.options.stickToBottom }
    set { update { $0.stickToBottom = newValue } }
  }

  /// Called with where the list is, on every scroll and page. Save the
  /// latest (debounced) to `restore(_:)` it later.
  public var onScrollStateChange: ((ScrollState) -> Void)? {
    get { core.options.onScrollStateChange }
    set { update { $0.onScrollStateChange = newValue } }
  }

  /// Called when the list has been idle for the settle time.
  public var onSettled: (() -> Void)? {
    get { core.options.onSettled }
    set { update { $0.onSettled = newValue } }
  }

  /// New queries, as when they capture state other than the list context
  /// that changed.
  public func setQueries(page: @escaping PageQuery, row: @escaping RowQuery) {
    pageQuery = page
    rowQuery = row
    single.close()
    main.close()
    after.close()
    sync()
  }

  private func update(_ change: (inout VirtualizerOptions<ListContext, Row, Key, StartRow>) -> Void)
  {
    var options = core.options
    change(&options)
    core.setOptions(options)
    sync()
  }

  // MARK: Scrolling

  /// Scrolls the row with `id` (what `rowQuery` takes) into view, loading
  /// it first if needed. An id with no row does nothing.
  public func scrollToItem(_ id: String, align: VirtualScrollAlignment = .auto) {
    core.scrollToItem(id, align: align)
  }

  /// Scrolls the rendered row with `key` into view.
  public func scrollToRow(key: Key, align: VirtualScrollAlignment = .auto) {
    core.scrollToRow(key: key, align: align)
  }

  /// The first loaded row in the viewport.
  public func firstVisibleItem() -> VirtualItem<Row, Key>? { core.firstVisibleItem() }

  /// The last loaded row in the viewport.
  public func lastVisibleItem() -> VirtualItem<Row, Key>? { core.lastVisibleItem() }

  /// Puts the list back where `state` says.
  public func restore(_ state: ScrollState) { core.restore(state) }

  // MARK: The adapter's side

  /// Drives `host`'s scrolling.
  public func attach(_ host: any VirtualHost) { core.attach(host) }

  public func detach() { core.detach() }

  /// Stops driving `host`, if it's the host it drives (nil: a host that's
  /// gone): another list may have taken the virtualizer over. Whether it
  /// was.
  package func detach(_ host: (any VirtualHost)?) -> Bool {
    guard core.isAttached(to: host) else { return false }
    core.detach()
    return true
  }

  /// After the host rendered `snapshot`, or its rows changed height.
  public func afterUpdate() { core.afterUpdate() }

  /// The content moved.
  public func didScroll() { core.didScroll() }

  /// A jump or a restore hasn't landed yet.
  package var isLanding: Bool { core.isLanding }

  /// The viewport changed size.
  public func viewportDidResize() { core.viewportDidResize() }

  // MARK: Queries

  /// Runs the queries the core asks for and hands it their rows.
  private func sync() {
    if syncing {
      dirty = true
      return
    }
    syncing = true
    defer { syncing = false }
    repeat {
      dirty = false
      let inputs = core.queryInputs
      let context = core.options.listContext
      if let next = reopen(single, singleRequest(inputs).map { rowQuery($0, context) }) {
        single = next
      }
      let pages = pageRequests(
        inputs, toStartRow: core.options.toStartRow, singleRow: single.rows ?? nil)
      if let next = reopen(main, pages.main.map { pageQuery($0, context) }) { main = next }
      if let next = reopen(after, pages.after.map { pageQuery($0, context) }) { after = next }
      core.setRows(
        assembleRows(
          inputs,
          RowsQueryResults(
            singleRow: single.rows ?? nil, singleComplete: single.complete, mainRows: main.rows,
            mainComplete: main.complete, afterRows: after.rows, afterComplete: after.complete)))
    } while dirty
    error = single.error ?? main.error ?? after.error
    snapshot = core.snapshot
    onSnapshot?()
  }

  /// The slot for `request`: nil when `slot` already runs it.
  private func reopen<R: QueryResult>(
    _ slot: Slot<R>, _ request: QueryRequest<Row, R, Context>?
  ) -> Slot<R>? where R.Row == Row {
    let key = request.map { SlotKey(name: $0.name, args: $0.args) }
    if slot.key == key { return nil }
    var old = slot
    old.close()
    var next = Slot<R>()
    next.key = key
    guard let request, let key else { return next }
    do {
      let view = try zero.materialize(request, ttl: ttl)
      next.view = view
      next.take(view.data, view.resultType, view.error)
      // Its first call, now, is the data just taken.
      var first = true
      next.remove = view.addListener { [weak self, weak view] value, type, error in
        if first {
          first = false
          return
        }
        guard let self, let view, !view.isDestroyed else { return }
        self.receive(value, type, error, into: key)
      }
    } catch {
      next.error = error
    }
    return next
  }

  private func receive(
    _ value: ViewValue, _ type: ResultType, _ queryError: ErroredQuery?, into key: SlotKey
  ) {
    if single.key == key { single.take(value, type, queryError) }
    if main.key == key { main.take(value, type, queryError) }
    if after.key == key { after.take(value, type, queryError) }
    sync()
  }
}

private struct SlotKey: Equatable {
  var name: String
  var args: [JSONValue]
}

/// One live query of the virtualizer.
@MainActor
private struct Slot<R: QueryResult> {
  var key: SlotKey?
  var view: MaterializedView?
  var remove: () -> Void = {}
  var rows: R?
  var complete = false
  var error: (any Error)?

  mutating func take(_ value: ViewValue, _ type: ResultType, _ queryError: ErroredQuery?) {
    do {
      rows = try R(view: value)
      // An errored query has nothing more coming: done, as far as paging goes.
      complete = type == .complete || type == .error
      error = queryError.map { QueryError($0) }
    } catch {
      self.error = error
    }
  }

  mutating func close() {
    remove()
    remove = {}
    view?.destroy()
    view = nil
    key = nil
    rows = nil
    complete = false
    error = nil
  }
}
