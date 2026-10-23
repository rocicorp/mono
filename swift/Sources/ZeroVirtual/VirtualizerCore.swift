import Zero

/// Where `scrollToItem` puts a row in the viewport.
public enum VirtualScrollAlignment: Sendable, Hashable {
  /// The least scroll that brings the row fully into view: none when it
  /// already is. A row taller than the viewport goes to the top.
  case auto
  /// The row's top at the viewport's top.
  case start
  /// The row in the middle of the viewport.
  case center
  /// The row's bottom at the viewport's bottom.
  case end
}

/// Who keeps what's on screen in place when rows above it are added,
/// removed, or change height.
public enum VirtualAnchoring: Sendable, Hashable {
  /// The scroll view does, as SwiftUI's `scrollPosition` does.
  case native
  /// The virtualizer does: it pins a row in view and scrolls by however far
  /// that row moves.
  case manual
}

/// A loaded row to render.
public struct VirtualItem<Row, Key: Hashable> {
  /// The row's position in the list as far as the virtualizer knows it,
  /// counted from the list's start once that's loaded.
  public var index: Int
  public var key: Key
  public var row: Row
}

extension VirtualItem: Sendable where Row: Sendable, Key: Sendable {}

/// Where the list was: what to save so it can be put back
/// (`ScrollHistoryState`).
public struct VirtualScrollState<StartRow: Equatable & Sendable, ListContext: Equatable & Sendable>:
  Equatable, Sendable
{
  public var anchor: VirtualAnchor<StartRow>
  public var scrollOffset: Double
  public var estimatedTotal: Int
  public var hasReachedStart: Bool
  public var hasReachedEnd: Bool
  /// The list the state is of. A state of another list isn't restored.
  public var listContext: ListContext

  public init(
    anchor: VirtualAnchor<StartRow>, scrollOffset: Double, estimatedTotal: Int,
    hasReachedStart: Bool, hasReachedEnd: Bool, listContext: ListContext
  ) {
    self.anchor = anchor
    self.scrollOffset = scrollOffset
    self.estimatedTotal = estimatedTotal
    self.hasReachedStart = hasReachedStart
    self.hasReachedEnd = hasReachedEnd
    self.listContext = listContext
  }
}

extension VirtualScrollState: Codable where StartRow: Codable, ListContext: Codable {}

/// A rendered row's frame, in content coordinates: from the top of the
/// scroll view's content, which includes the space standing in for the rows
/// above the loaded ones.
public struct VirtualRowFrame {
  public var index: Int
  public var key: AnyHashable
  public var minY: Double
  public var maxY: Double

  public init(index: Int, key: AnyHashable, minY: Double, maxY: Double) {
    self.index = index
    self.key = key
    self.minY = minY
    self.maxY = maxY
  }
}

/// The scroll view a virtualizer drives: the DOM, for zero-virtual's core.
///
/// The host reports every change of its offset with `didScroll()`,
/// including the virtualizer's own `scroll(to:)` and clamping as the content
/// shrinks, as `scrollViewDidScroll` does.
@MainActor
public protocol VirtualHost: AnyObject {
  /// How far the content is scrolled: the viewport's top in content
  /// coordinates.
  var scrollOffset: Double { get }
  /// Scrolls to `offset`, clamped to the content. Calling `didScroll()`
  /// before returning is fine.
  func scroll(to offset: Double)
  /// The viewport's height.
  var viewportHeight: Double { get }
  /// Strips at the viewport's top and bottom that cover rows, such as a
  /// toolbar. `scrollToItem` lines rows up clear of them.
  var viewportInsets: (top: Double, bottom: Double) { get }
  /// The content's height.
  var contentHeight: Double { get }
  /// The rendered rows, in list order.
  func rowFrames() -> [VirtualRowFrame]
  /// A scroll and a new snapshot are applied in the same layout pass, and a
  /// scroll doesn't stop a fling: the list can scroll while it may be
  /// moving. False by default.
  var scrollsWithLayout: Bool { get }
}

extension VirtualHost {
  public var scrollsWithLayout: Bool { false }
}

/// What the list renders (`VirtualizerSnapshot`).
public struct VirtualSnapshot<Row, Key: Hashable> {
  /// The loaded rows.
  public var items: [VirtualItem<Row, Key>]
  /// The height standing in for the rows above the loaded ones.
  public var spaceBefore: Double
  /// The height standing in for the rows below the loaded ones.
  public var spaceAfter: Double
  /// The queries of the loaded rows have their server answers.
  public var complete: Bool
  /// The list has no rows, as far as it's loaded.
  public var rowsEmpty: Bool
  /// The permalink's row doesn't exist.
  public var permalinkNotFound: Bool
  /// How many rows the list has, at least: the loaded ones and those seen
  /// above and below them.
  public var estimatedTotal: Int
  /// How many rows the list has, once both its ends have been seen.
  public var total: Int?
  /// The list has been idle for the settle time.
  public var settled: Bool

  public init() {
    items = []
    spaceBefore = 0
    spaceAfter = 0
    complete = false
    rowsEmpty = true
    permalinkNotFound = false
    estimatedTotal = 0
    total = nil
    settled = false
  }
}

extension VirtualSnapshot: Sendable where Row: Sendable, Key: Sendable {}

/// The options of the paging core (`VirtualizerOptions`).
package struct VirtualizerOptions<
  ListContext: Equatable & Sendable, Row, Key: Hashable, StartRow: Equatable & Sendable
> {
  package var estimatedRowHeight: Double
  package var overscan = 5
  package var anchoring = VirtualAnchoring.manual
  package var rowKey: (Row) -> Key
  package var toStartRow: (Row) -> StartRow
  package var listContext: ListContext
  package var permalinkID: String?
  /// Milliseconds idle before the list is settled.
  package var settleTime: Double = 2000
  package var minPageSize: Int?
  /// A position to start at, as saved from `onScrollStateChange`. Later
  /// restores go through `restore(_:)`.
  package var scrollState: VirtualScrollState<StartRow, ListContext>?
  package var onScrollStateChange: ((VirtualScrollState<StartRow, ListContext>) -> Void)?
  package var onSettled: (() -> Void)?
  /// Follow the bottom while the list is parked there: rows added below, or
  /// a row there growing, scroll into view (`useStickToBottom`).
  package var stickToBottom = false
  /// How near the bottom counts as at it.
  package var stickSlack = 4.0

  package init(
    estimatedRowHeight: Double, rowKey: @escaping (Row) -> Key,
    toStartRow: @escaping (Row) -> StartRow, listContext: ListContext
  ) {
    self.estimatedRowHeight = estimatedRowHeight
    self.rowKey = rowKey
    self.toStartRow = toStartRow
    self.listContext = listContext
  }
}

// One row of loading skeleton above a window opened mid-list.
private let skeletonRows = 1
private let defaultMinPageSize = 50
// Commits a permalink must be reported missing before the list gives up on
// it: the first may still carry the previous lookup's answer.
private let permalinkMissingCommits = 2

/// The paging and anchoring of a virtual list, without its queries or its
/// views: zero-virtual's `ZeroVirtualizer` core. Rows come in through
/// `setRows`, the scroll view through `attach`, and the queries to run are
/// `queryInputs`.
///
/// The host calls `afterUpdate()` once it has rendered a new snapshot,
/// `didScroll()` when the content moves, and `viewportDidResize()`.
@MainActor
package final class VirtualizerCore<
  ListContext: Equatable & Sendable, Row, Key: Hashable, StartRow: Equatable & Sendable
> {
  package typealias Options = VirtualizerOptions<ListContext, Row, Key, StartRow>
  package typealias Anchor = VirtualAnchor<StartRow>
  package typealias ScrollState = VirtualScrollState<StartRow, ListContext>

  private struct Paging {
    var estimatedTotal: Int
    var hasReachedStart: Bool
    var hasReachedEnd: Bool
    var anchor: Anchor
    var listContext: ListContext
  }

  private struct PersistState: Equatable {
    var anchor: Anchor
    var estimatedTotal: Int
    var hasReachedStart: Bool
    var hasReachedEnd: Bool
  }

  private enum Source { case option, imperative }

  /// A scroll to a row that hasn't landed.
  private struct PendingScroll {
    var id: String?
    /// The rendered row's key, when the request named it or a lookup found it.
    var rowKey: AnyHashable?
    var align: VirtualScrollAlignment
    var source: Source
  }

  /// Where the viewport is going: a row (a jump) or an offset (a restore)
  /// owns the position until it lands; a probe is a jump still looking its
  /// row up.
  private enum Target {
    case probe(PendingScroll)
    case row(PendingScroll)
    case offset(Double)

    var isProbe: Bool {
      if case .probe = self { return true }
      return false
    }
  }

  private let scheduler: any Scheduler
  private(set) package var options: Options
  private var rows: RowsSnapshot<Row, StartRow> = .empty
  private var paging: Paging
  private var pageSize: Int
  private(set) package var settled = false

  private var listeners: [Int: () -> Void] = [:]
  private var nextListener = 0
  private var version = 0
  private var cachedSnapshot: (version: Int, snapshot: VirtualSnapshot<Row, Key>)?

  private weak var host: (any VirtualHost)?
  private var programmaticScroll = false
  // Entry calls under way, and a scroll the host reported during our own
  // write, delivered when the outermost one ends, as a browser delivers the
  // scroll event after the task that wrote the offset.
  private var depth = 0
  private var writing = false
  private var scrollDuringWrite = false

  private var anchorKey: AnyHashable?
  // The reference row's top, in content coordinates.
  private var anchorOffset = 0.0
  // From a reset until the new window's rows are in.
  private var anchorSuppressed = false
  // How far the space before the rows is off its estimate. Rows above the
  // reference row that load, go, or come out another height than estimated
  // would move it; the space takes up the difference instead of a scroll,
  // which would stop a fling. Space short of the estimate is given back with
  // a scroll once the list rests.
  private var spaceAdjust = 0.0
  // The list opened away from its start (a permalink, a restore) and hasn't
  // been there since: its indexes count from where it opened.
  private var openedAway: Bool
  private var idleTimer: (any ScheduledTimer)?
  // The space is being given back: compensation scrolls.
  private var reconciling = false
  private var settleTimer: (any ScheduledTimer)?
  private var target: Target?
  // The target's position has been applied once: the viewport is where it
  // goes, though its window may not have settled (offline, it never does).
  private var placed = false
  /// A jump or a restore hasn't put the viewport where it goes yet: the
  /// view holds off drawing the rows it opens with until it has.
  package var isLanding: Bool { target != nil && !placed }
  private var permalinkMissing = 0

  // A restore not yet applied: from `scrollState` at first, then
  // `restore(_:)`.
  private var pendingRestore: ScrollState?
  private var appliedPermalinkID: String?
  private var lastPersisted: PersistState?
  private var lastWritten: ScrollState?
  private var lastSettleContext: ListContext
  // Whether the list was at the bottom as of the last scroll, so growth
  // since then re-pins it.
  private var stuck = false

  package init(_ options: Options, scheduler: any Scheduler) {
    self.options = options
    self.scheduler = scheduler
    pageSize = Self.minPageSize(options)
    let restore = Self.restorable(options)
    if let restore {
      paging = Paging(
        estimatedTotal: restore.estimatedTotal, hasReachedStart: restore.hasReachedStart,
        hasReachedEnd: restore.hasReachedEnd, anchor: restore.anchor,
        listContext: options.listContext)
    } else if let id = options.permalinkID {
      paging = Self.permalinkPaging(id, options.listContext)
    } else {
      paging = Paging(
        estimatedTotal: skeletonRows, hasReachedStart: false, hasReachedEnd: false, anchor: .top,
        listContext: options.listContext)
    }
    openedAway = restore.map { !$0.hasReachedStart } ?? (options.permalinkID != nil)
    if let id = options.permalinkID, restore == nil {
      target = .row(PendingScroll(id: id, align: .start, source: .option))
    }
    appliedPermalinkID = options.permalinkID
    lastSettleContext = options.listContext
    pendingRestore = restore
  }

  /// Puts the list back where `state` says, as on a navigation back. A
  /// state of another list context is ignored, and so is the one this
  /// core reported last, which is where the list already is.
  package func restore(_ state: ScrollState) {
    guard state.listContext == options.listContext, state != lastWritten else { return }
    pendingRestore = state
    version += 1
  }

  // MARK: - Host-facing surface

  /// Takes new options without notifying.
  package func setOptions(_ new: Options) {
    let old = options
    options = new
    if old.estimatedRowHeight != new.estimatedRowHeight { version += 1 }
  }

  /// Takes new rows without notifying.
  package func setRows(_ new: RowsSnapshot<Row, StartRow>) {
    let old = rows
    let oldSpace = manual && host != nil ? snapshot.spaceBefore : nil
    rows = new
    version += 1
    if let oldSpace { carrySpace(from: old, oldSpace) }
  }

  /// Keeps the space before the rows where it was through new rows: less
  /// the rows added above the first one, by estimate, or more the rows gone
  /// from above it, as laid out. Their index, which paging renumbers, and
  /// the anchor's kind don't move what's on screen.
  private func carrySpace(from old: RowsSnapshot<Row, StartRow>, _ oldSpace: Double) {
    guard isListContextCurrent, !old.rowsEmpty, !rows.rowsEmpty,
      let oldFirst = old.rowAt(old.firstRowIndex).map(options.rowKey),
      let newFirst = rows.rowAt(rows.firstRowIndex).map(options.rowKey)
    else { return }
    func position(of key: Key, in r: RowsSnapshot<Row, StartRow>) -> Int? {
      for i in 0..<r.rowsLength where r.rowAt(r.firstRowIndex + i).map(options.rowKey) == key {
        return i
      }
      return nil
    }
    var space = oldSpace
    if oldFirst == newFirst {
      // Same first row: the space stays as it was.
    } else if let added = position(of: oldFirst, in: rows) {
      space -= Double(added) * rowEstimate
    } else if let removed = position(of: newFirst, in: old) {
      if let host, let from = frame(host, key: AnyHashable(oldFirst)),
        let to = frame(host, key: AnyHashable(newFirst))
      {
        space += to.minY - from.minY
      } else {
        space += Double(removed) * rowEstimate
      }
    } else {
      return
    }
    spaceAdjust = space - estimatedSpaceBefore
  }

  /// The queries the rows must answer.
  package var queryInputs: RowsQueryInputs<StartRow> {
    var probe: String?
    if case .probe(let p) = target { probe = p.id }
    return RowsQueryInputs(
      pageSize: pageSize, anchor: effectiveAnchor, settled: settled, probeID: probe)
  }

  /// Calls `listener` whenever the snapshot or the query inputs change.
  @discardableResult
  package func subscribe(_ listener: @escaping () -> Void) -> () -> Void {
    let id = nextListener
    nextListener += 1
    listeners[id] = listener
    return { [weak self] in self?.listeners[id] = nil }
  }

  /// What to render.
  package var snapshot: VirtualSnapshot<Row, Key> {
    if let cachedSnapshot, cachedSnapshot.version == version { return cachedSnapshot.snapshot }
    let s = buildSnapshot()
    cachedSnapshot = (version, s)
    return s
  }

  /// Drives `host`, replacing any other.
  package func attach(_ host: any VirtualHost) {
    if self.host === host { return }
    detach()
    self.host = host
    measureStuck()
    withNotify { resetSettleTimer() }
  }

  package func isAttached(to host: (any VirtualHost)?) -> Bool { self.host === host }

  package func detach() {
    host = nil
    settleTimer?.cancel()
    settleTimer = nil
    idleTimer?.cancel()
    idleTimer = nil
  }

  /// After the host rendered a new snapshot, or its rows changed height.
  package func afterUpdate() {
    withNotify { afterUpdateImpl() }
  }

  /// The viewport changed size.
  package func viewportDidResize() {
    withNotify {
      repinToBottom()
      evaluate()
    }
  }

  /// The content moved.
  package func didScroll() {
    if writing {
      scrollDuringWrite = true
      return
    }
    onScroll()
  }

  /// Scrolls the row with `id`, the id the single-row query takes, into
  /// view, loading it first if it isn't loaded. A rendered row whose key is
  /// `id` scrolls without a lookup. An id with no row does nothing.
  package func scrollToItem(_ id: String, align: VirtualScrollAlignment = .auto) {
    guard !id.isEmpty else { return }
    withNotify { startOrScroll(PendingScroll(id: id, align: align, source: .imperative)) }
  }

  /// Scrolls the rendered row with `key` into view. A row that isn't
  /// rendered is out of reach: scroll to it by id.
  package func scrollToRow(key: Key, align: VirtualScrollAlignment = .auto) {
    withNotify {
      guard let host, frame(host, key: AnyHashable(key)) != nil else { return }
      setTarget(.row(PendingScroll(rowKey: AnyHashable(key), align: align, source: .imperative)))
      land()
    }
  }

  /// The first loaded row in the viewport.
  package func firstVisibleItem() -> VirtualItem<Row, Key>? { visibleItem(first: true) }

  /// The last loaded row in the viewport.
  package func lastVisibleItem() -> VirtualItem<Row, Key>? { visibleItem(first: false) }

  // MARK: - Requests

  private func startOrScroll(_ request: PendingScroll) {
    if let host, findTarget(host, request) != nil {
      setTarget(.row(request))
      land()
      return
    }
    // A repeat while its page is still loading waits for it.
    if let id = request.id, isListContextCurrent, isTargeting(id), !rows.complete {
      setTarget(.row(request))
      return
    }
    // A loaded list checks the row exists before giving itself up for it.
    if request.rowKey == nil, !paging.anchor.isPermalink, !rows.rowsEmpty, isListContextCurrent {
      setTarget(.probe(request))
      return
    }
    guard let id = request.id else { return }
    setTarget(.row(request))
    anchorKey = nil
    anchorSuppressed = true
    spaceAdjust = 0
    setPaging(Self.permalinkPaging(id, options.listContext))
  }

  // A probe is a query input; other targets aren't.
  private func setTarget(_ new: Target?) {
    if target?.isProbe == true || new?.isProbe == true { version += 1 }
    target = new
    placed = false
  }

  private func resolveProbe() {
    guard case .probe(let probe) = target else { return }
    guard isListContextCurrent else {
      setTarget(nil)
      return
    }
    guard let lookup = rows.lookup, lookup.id == probe.id, lookup.complete else { return }
    guard let row = lookup.row else {
      setTarget(nil)
      return
    }
    var found = probe
    found.rowKey = AnyHashable(options.rowKey(row))
    startOrScroll(found)
  }

  private func findTarget(_ host: any VirtualHost, _ request: PendingScroll) -> VirtualRowFrame? {
    if let id = request.id, let f = frame(host, key: AnyHashable(id)) { return f }
    if let key = request.rowKey { return frame(host, key: key) }
    return nil
  }

  private func frame(_ host: any VirtualHost, key: AnyHashable) -> VirtualRowFrame? {
    host.rowFrames().first { $0.key == key }
  }

  private func afterUpdateImpl() {
    if options.listContext != lastSettleContext {
      lastSettleContext = options.listContext
      resetSettleTimer()
    }
    // Space that takes up rows moving moves them back on the next layout:
    // the rows aren't where paging would read them until then.
    var respaced = measureAndCompensate()
    if dropSpaceAtStart() { respaced = true }
    restoreOrReset()
    updatePageSize()
    land()
    recoverFromMissingPermalink()

    liftAnchorSuppression()
    updatePaging()
    resolveProbe()
    repinToBottom()
    if !respaced { evaluatePaging() }
    persist(onlyIfChanged: true)
  }

  // MARK: - Stick to bottom

  private func measureStuck() {
    guard let host else { return }
    stuck = host.contentHeight - host.scrollOffset - host.viewportHeight <= options.stickSlack
  }

  private func repinToBottom() {
    guard options.stickToBottom, stuck, !landing, let host else { return }
    setScrollOffset(host.contentHeight - host.viewportHeight)
    stuck = true
  }

  // MARK: - Derived

  private static func minPageSize(_ options: Options) -> Int {
    guard let min = options.minPageSize else { return defaultMinPageSize }
    return makeEven(max(2, min))
  }

  private static func restorable(_ options: Options) -> ScrollState? {
    guard let state = options.scrollState, state.listContext == options.listContext else {
      return nil
    }
    return state
  }

  private static func permalinkPaging(_ id: String, _ context: ListContext) -> Paging {
    Paging(
      estimatedTotal: skeletonRows, hasReachedStart: false, hasReachedEnd: false,
      anchor: .permalink(index: skeletonRows, id: id), listContext: context)
  }

  private var isListContextCurrent: Bool { paging.listContext == options.listContext }

  // Queries use a new list context at once, before the reset commits.
  private var effectiveAnchor: Anchor {
    if isListContextCurrent { return paging.anchor }
    if let id = options.permalinkID { return .permalink(index: skeletonRows, id: id) }
    return .top
  }

  // How long without a scroll counts as at rest, in milliseconds: longer
  // than the gap between a fling's frames.
  private static var idleTime: Double { 150 }

  private var rowEstimate: Double { max(1, options.estimatedRowHeight) }

  // The space before the rows, as their count estimates it. A list that
  // opened away from its start counts from where it opened, which estimates
  // nothing, so it keeps at least a reserve for the space to take changes
  // above the rows out of. None at the start.
  private var estimatedSpaceBefore: Double {
    if rows.atStart { return 0 }
    let estimate = Double(max(0, rows.firstRowIndex)) * rowEstimate
    guard manual, openedAway, !rows.rowsEmpty else { return estimate }
    return max(estimate, Double(3 * pageSize) * rowEstimate)
  }

  private var manual: Bool { options.anchoring == .manual }

  private var effectiveEstimatedTotal: Int {
    let end = rows.firstRowIndex + rows.rowsLength
    if rows.atEnd && rows.atStart && rows.complete { return rows.rowsLength }
    return max(paging.estimatedTotal, end)
  }

  private func buildSnapshot() -> VirtualSnapshot<Row, Key> {
    let total = effectiveEstimatedTotal
    let end = rows.firstRowIndex + rows.rowsLength
    let after = rows.atEnd ? 0 : max(0, total - end)
    var items: [VirtualItem<Row, Key>] = []
    items.reserveCapacity(rows.rowsLength)
    for index in rows.firstRowIndex..<end {
      guard let row = rows.rowAt(index) else { continue }
      items.append(VirtualItem(index: index, key: options.rowKey(row), row: row))
    }
    var s = VirtualSnapshot<Row, Key>()
    s.items = items
    // At the start, space left over from above stands before the first row
    // until the list rests.
    s.spaceBefore = max(0, estimatedSpaceBefore + spaceAdjust)
    s.spaceAfter = Double(after) * rowEstimate
    s.complete = rows.complete
    s.rowsEmpty = rows.rowsEmpty
    s.permalinkNotFound = rows.permalinkNotFound
    s.estimatedTotal = total
    s.total =
      rows.atStart && rows.atEnd
      ? rows.rowsLength
      : paging.hasReachedStart && paging.hasReachedEnd ? paging.estimatedTotal : nil
    s.settled = settled
    return s
  }

  private func notify() {
    for listener in listeners.values { listener() }
  }

  private func withNotify(_ body: () -> Void) {
    let before = version
    depth += 1
    body()
    depth -= 1
    if depth == 0, scrollDuringWrite {
      scrollDuringWrite = false
      onScroll()
    }
    if version != before { notify() }
  }

  private func setPaging(_ next: Paging) {
    paging = next
    if next.anchor == .top || next.hasReachedStart {
      openedAway = false
    } else if next.anchor.isPermalink {
      openedAway = true
    }
    version += 1
  }

  private func setAnchor(_ anchor: Anchor) {
    // Paging can pick the same anchor every commit.
    if paging.anchor == anchor { return }
    var next = paging
    next.anchor = anchor
    setPaging(next)
  }

  // MARK: - Geometry

  private func visibleIndexRange(_ host: any VirtualHost) -> (first: Int, last: Int)? {
    let top = host.scrollOffset
    let bottom = top + host.viewportHeight
    var first = Int.max
    var last = Int.min
    for f in host.rowFrames() {
      if f.minY >= bottom { break }
      if f.maxY > top && f.minY < bottom {
        first = min(first, f.index)
        last = max(last, f.index)
      }
    }
    return first == .max ? nil : (first, last)
  }

  private func visibleItem(first: Bool) -> VirtualItem<Row, Key>? {
    guard let host, let range = visibleIndexRange(host) else { return nil }
    let index = first ? range.first : range.last
    return snapshot.items.first { $0.index == index }
  }

  // Scrolls unless already there; whether it wrote.
  @discardableResult
  private func setScrollOffset(_ offset: Double) -> Bool {
    guard let host, host.scrollOffset.rounded() != offset.rounded() else { return false }
    programmaticScroll = true
    writing = true
    host.scroll(to: offset)
    writing = false
    if depth == 0, scrollDuringWrite {
      scrollDuringWrite = false
      onScroll()
    }
    return true
  }

  // MARK: - Anchoring

  // The first row reaching below the viewport's top, as native anchoring
  // picks it.
  private func refreshAnchor() {
    guard let host else { return }
    let top = host.scrollOffset
    let ref = host.rowFrames().first { $0.maxY > top + 0.5 }
    anchorKey = ref?.key
    anchorOffset = ref?.minY ?? 0
  }

  private func compensate(_ delta: Double) {
    guard let host else { return }
    anchorOffset += delta
    setScrollOffset(host.scrollOffset + delta)
  }

  /// Keeps the anchor row in place; true when the space before the rows
  /// took up its move, which the next layout applies.
  @discardableResult
  private func measureAndCompensate() -> Bool {
    guard manual, !anchorSuppressed, isListContextCurrent, !landing, let host else {
      return false
    }
    // At the top, rows added above are revealed, not compensated away.
    if host.scrollOffset <= 0 {
      refreshAnchor()
      return false
    }
    guard let key = anchorKey, let ref = frame(host, key: key) else {
      refreshAnchor()
      return false
    }
    let delta = ref.minY - anchorOffset
    defer { reconciling = false }
    if abs(delta) < 0.5 { return false }
    // Taken up by the space before the rows, which moves them back, when
    // it can be: the scroll view isn't touched, so a fling carries on.
    let space = snapshot.spaceBefore - delta
    if !reconciling, space >= 0 {
      spaceAdjust = space - estimatedSpaceBefore
      version += 1
      return true
    }
    compensate(delta)
    return false
  }

  /// At the start nothing stands before the first row: on a host that
  /// scrolls with its layout, space left over there (from rows that came
  /// out shorter, or carried over as the first page arrived) comes out at
  /// once, with a scroll by as much in the same pass, so no frame shows a
  /// gap above the first row, even mid-fling. What the offset can't take,
  /// near the top, waits for the list to rest.
  private func dropSpaceAtStart() -> Bool {
    guard manual, rows.atStart, spaceAdjust > 0, isListContextCurrent, !landing, let host,
      host.scrollsWithLayout
    else { return false }
    let removed = min(spaceAdjust, max(0, host.scrollOffset))
    guard removed >= 0.5 else { return false }
    spaceAdjust -= removed
    anchorOffset -= removed
    version += 1
    setScrollOffset(host.scrollOffset - removed)
    return true
  }

  // Once scrolling stops, space short of its estimate goes back to it, and
  // the list scrolls by the difference so nothing on screen moves.
  private func resetIdleTimer() {
    idleTimer?.cancel()
    idleTimer = scheduler.schedule(after: Self.idleTime) { [weak self] in
      // Space short of its estimate is given back, for the next change to
      // come out of. Space over it stays, as taking it back shrinks the
      // content under the offset, which the view clamps before the list can
      // scroll; but at the start, where nothing stands before the first row.
      guard let self, self.spaceAdjust < 0 || (self.rows.atStart && self.spaceAdjust > 0)
      else { return }
      self.withNotify {
        self.spaceAdjust = 0
        self.reconciling = true
        self.version += 1
      }
    }
  }

  private func liftAnchorSuppression() {
    if anchorSuppressed && rowsSettled {
      anchorSuppressed = false
      refreshAnchor()
    }
  }

  // The rows answer the query as it is now: its anchor and page size.
  private var rowsAnswerQuery: Bool {
    isListContextCurrent && rows.pageSize == pageSize && rows.anchor == paging.anchor
  }

  private var rowsSettled: Bool { rowsAnswerQuery && rows.complete }

  private var landing: Bool {
    guard let target else { return false }
    return !target.isProbe
  }

  private func landed() {
    if !anchorSuppressed { refreshAnchor() }
    // A scroll that lands at the bottom, such as to the latest row, follows
    // from there; one that lands above it doesn't.
    measureStuck()
    persist()
  }

  private func onScroll() {
    let programmatic = programmaticScroll
    programmaticScroll = false
    if !programmatic, case .offset = target { target = nil }
    // Our own scrolls keep it as it was: compensation, or the re-pin.
    if !programmatic { measureStuck() }
    if manual && !programmatic { refreshAnchor() }
    withNotify {
      resetSettleTimer()
      if manual { resetIdleTimer() }
      evaluate()
      persist()
    }
  }

  private func evaluate() {
    updatePageSize()
    evaluatePaging()
  }

  // MARK: - Settling

  private func resetSettleTimer() {
    if settled {
      settled = false
      version += 1
    }
    settleTimer?.cancel()
    settleTimer = scheduler.schedule(after: options.settleTime) { [weak self] in
      guard let self else { return }
      self.settled = true
      self.version += 1
      self.options.onSettled?()
      self.notify()
    }
  }

  // MARK: - Paging

  // About three viewports of rows.
  private func updatePageSize() {
    let min = Self.minPageSize(options)
    let height = host?.viewportHeight ?? 0
    let size =
      height > 0 ? max(min, makeEven(Int((height / rowEstimate).rounded(.up)) * 3)) : min
    if size > pageSize {
      pageSize = size
      version += 1
    }
  }

  // Takes in the loaded window's extent, and renumbers it.
  private func updatePaging() {
    guard rowsAnswerQuery else { return }
    let s = paging
    var anchor = s.anchor
    var total = s.estimatedTotal
    let reachedStart = s.hasReachedStart || rows.atStart
    let reachedEnd = s.hasReachedEnd || rows.atEnd
    if rows.complete {
      let extent =
        rows.atStart && rows.atEnd ? rows.rowsLength : rows.firstRowIndex + rows.rowsLength
      if extent > total { total = extent }
    }
    if !rows.rowsEmpty {
      if rows.firstRowIndex < 0 {
        // Start the window at 0, or a skeleton row down with more above.
        let offset = rows.atStart ? -rows.firstRowIndex : skeletonRows - rows.firstRowIndex
        anchor = anchor.with(index: anchor.index + offset)
        total += offset
      } else if rows.atStart && rows.firstRowIndex > 0 {
        anchor = .top
        total -= rows.firstRowIndex
      }
    }
    if anchor != s.anchor || total != s.estimatedTotal || reachedStart != s.hasReachedStart
      || reachedEnd != s.hasReachedEnd
    {
      setPaging(
        Paging(
          estimatedTotal: total, hasReachedStart: reachedStart, hasReachedEnd: reachedEnd,
          anchor: anchor, listContext: s.listContext))
    }
  }

  private func restoreOrReset() {
    let restore = pendingRestore.flatMap { $0.listContext == options.listContext ? $0 : nil }
    let permalinkID = options.permalinkID
    let scrollStateChanged = restore != nil
    let permalinkChanged = permalinkID != appliedPermalinkID

    // Not before a scroll view: the scroll would be lost.
    if (restore != nil || permalinkID != nil) && host == nil { return }

    // A jump outranks a restore.
    if let t = target {
      var jump = false
      switch t {
      case .probe: jump = true
      case .row(let p): jump = p.source == .imperative
      case .offset: break
      }
      if jump {
        if isListContextCurrent && !permalinkChanged {
          pendingRestore = nil
          appliedPermalinkID = permalinkID
          return
        }
        setTarget(nil)
      }
    }

    pendingRestore = nil
    appliedPermalinkID = permalinkID

    if isListContextCurrent && !scrollStateChanged && !permalinkChanged { return }

    if !isListContextCurrent {
      anchorKey = nil
      anchorSuppressed = true
    }
    spaceAdjust = 0

    if case .offset = target { setTarget(nil) }
    if let restore {
      setTarget(.offset(restore.scrollOffset))
      if setScrollOffset(restore.scrollOffset) { anchorKey = nil }
      setPaging(
        Paging(
          estimatedTotal: restore.estimatedTotal, hasReachedStart: restore.hasReachedStart,
          hasReachedEnd: restore.hasReachedEnd, anchor: restore.anchor,
          listContext: options.listContext))
      openedAway = !restore.hasReachedStart
    } else if let permalinkID {
      // A visible permalink row stays put; any other is scrolled to.
      let targetFrame = host.flatMap { frame($0, key: AnyHashable(permalinkID)) }
      var visible = false
      if let host, let f = targetFrame {
        visible = f.maxY > host.scrollOffset && f.minY < host.scrollOffset + host.viewportHeight
      }
      if !visible {
        let request = PendingScroll(id: permalinkID, align: .start, source: .option)
        if targetFrame != nil {
          setTarget(.row(request))
        } else {
          startOrScroll(request)
        }
      }
    } else {
      anchorKey = nil
      setScrollOffset(0)
      setPaging(
        Paging(
          estimatedTotal: 0, hasReachedStart: true, hasReachedEnd: false, anchor: .top,
          listContext: options.listContext))
    }
  }

  // A missing permalink on a cold load has no list to keep: go to the top.
  private func recoverFromMissingPermalink() {
    guard rows.permalinkNotFound, isListContextCurrent,
      case .permalink(_, let id) = paging.anchor, rows.lookup?.id == id
    else {
      permalinkMissing = 0
      return
    }
    permalinkMissing += 1
    if permalinkMissing < permalinkMissingCommits { return }
    permalinkMissing = 0
    setTarget(nil)
    anchorKey = nil
    setPaging(
      Paging(
        estimatedTotal: 0, hasReachedStart: true, hasReachedEnd: false, anchor: .top,
        listContext: options.listContext))
  }

  private func isTargeting(_ id: String) -> Bool {
    if case .permalink(_, let anchorID) = paging.anchor { return anchorID == id }
    return false
  }

  private func alignDelta(
    _ host: any VirtualHost, _ f: VirtualRowFrame, _ align: VirtualScrollAlignment
  ) -> Double {
    let insets = host.viewportInsets
    let top = host.scrollOffset + insets.top
    let bottom = host.scrollOffset + host.viewportHeight - insets.bottom
    switch align {
    case .start: return f.minY - top
    case .end: return f.maxY - bottom
    case .center: return (f.minY + f.maxY) / 2 - (top + bottom) / 2
    case .auto:
      if f.maxY - f.minY > bottom - top || f.minY < top { return f.minY - top }
      if f.maxY > bottom { return f.maxY - bottom }
      return 0
    }
  }

  // Puts the viewport where a jump or restore goes, every commit until its
  // window has settled.
  private func land() {
    guard let target else { return }
    switch target {
    case .probe: return
    case .offset(let offset):
      guard rowsAnswerQuery else { return }
      setScrollOffset(offset)
    case .row(let pending):
      guard let moved = landRow(pending) else { return }
      // A host that lays out with its scroll measures the rows it scrolled
      // to only then: the row may have been lined up by an estimate, and is
      // lined up again, measured, in the pass that lays the scroll out.
      if moved, host?.scrollsWithLayout == true {
        placed = true
        return
      }
    }
    placed = true
    if rowsSettled {
      setTarget(nil)
      landed()
    }
  }

  /// Lines the row up; whether it scrolled to, or nil when the row isn't
  /// there.
  private func landRow(_ pending: PendingScroll) -> Bool? {
    if pending.source == .option, pending.id != options.permalinkID {
      setTarget(nil)
      return nil
    }
    guard let host else { return nil }
    var f = findTarget(host, pending)
    if f == nil, pending.rowKey == nil, let id = pending.id {
      // The row's key, once its own page queries have completed.
      if let lookup = rows.lookup, lookup.id == id, let row = lookup.row, rows.complete {
        var found = pending
        found.rowKey = AnyHashable(options.rowKey(row))
        setTarget(.row(found))
        f = frame(host, key: found.rowKey!)
      }
    }
    guard let f else {
      // Don't wait for a row that's missing or whose query is gone.
      let missing = rows.permalinkNotFound && rows.lookup?.id == pending.id
      let abandoned = rows.complete && !(pending.id.map(isTargeting) ?? false)
      if missing || abandoned { setTarget(nil) }
      return nil
    }
    let delta = alignDelta(host, f, pending.align)
    guard abs(delta) > 1 else { return false }
    // Clamped to the content, it can't go all the way.
    let from = host.scrollOffset
    setScrollOffset(from + delta)
    return abs(host.scrollOffset - from) > 1
  }

  private func evaluatePaging() {
    guard rowsSettled, !rows.rowsEmpty, !programmaticScroll, !landing, let host else { return }
    let visible = visibleIndexRange(host)
    let threshold = max(options.overscan, nearPageEdgeThreshold(pageSize))

    func updateAnchorForEdge(_ targetIndex: Int, _ direction: VirtualDirection, _ offset: Int) {
      let index = boundIndex(targetIndex, rows.firstRowIndex, rows.rowsLength)
      let row = rows.rowAt(index)
      switch direction {
      case .forward:
        setAnchor(.forward(index: index + offset, startRow: row.map(options.toStartRow)))
      case .backward:
        guard let row else { preconditionFailure("a backward page starts at a loaded row") }
        setAnchor(.backward(index: index + offset, startRow: options.toStartRow(row)))
      }
    }

    guard let visible else {
      // A jump into the unloaded space pages toward it.
      guard let top = host.rowFrames().first?.minY else { return }
      if top >= host.scrollOffset + host.viewportHeight {
        if host.scrollOffset <= 0 || rows.atStart {
          setAnchor(.top)
        } else {
          updateAnchorForEdge(rows.firstRowIndex, .backward, 0)
        }
      } else {
        updateAnchorForEdge(rows.firstRowIndex + rows.rowsLength - 1, .forward, 1)
      }
      return
    }

    if rows.atStart && rows.firstRowIndex != 0 {
      setAnchor(.top)
      return
    }

    // A page must hold what's on screen with room on both sides, or paging
    // flips between its edges.
    let span = visible.last - visible.first + 1
    if pageSize < span + 4 * threshold {
      pageSize = makeEven(span + 4 * threshold)
      version += 1
      return
    }

    let fromStart = visible.first - rows.firstRowIndex
    let fromEnd = rows.firstRowIndex + rows.rowsLength - 1 - visible.last
    if !rows.atStart && fromStart <= threshold {
      updateAnchorForEdge(visible.last + 2 * threshold, .backward, 0)
      return
    }
    if !rows.atEnd && fromEnd <= threshold {
      updateAnchorForEdge(visible.first - 2 * threshold, .forward, 1)
    }
  }

  // MARK: - Persistence

  private var persistState: PersistState {
    PersistState(
      anchor: paging.anchor, estimatedTotal: effectiveEstimatedTotal,
      hasReachedStart: paging.hasReachedStart, hasReachedEnd: paging.hasReachedEnd)
  }

  // Scrolls report every time; updates only when paging changed.
  private func persist(onlyIfChanged: Bool = false) {
    if landing { return }
    let next = persistState
    if onlyIfChanged && lastPersisted == next { return }
    guard let host, isListContextCurrent, let report = options.onScrollStateChange else { return }
    lastPersisted = next
    let state = ScrollState(
      // Where the list would be with the space as estimated, as a restore
      // lays it out.
      anchor: next.anchor,
      scrollOffset: host.scrollOffset - (snapshot.spaceBefore - estimatedSpaceBefore),
      estimatedTotal: next.estimatedTotal,
      hasReachedStart: next.hasReachedStart, hasReachedEnd: next.hasReachedEnd,
      listContext: options.listContext)
    lastWritten = state
    report(state)
  }
}

private func boundIndex(_ target: Int, _ first: Int, _ length: Int) -> Int {
  if length == 0 { return first }
  return max(first, min(first + length - 1, target))
}

private func nearPageEdgeThreshold(_ pageSize: Int) -> Int {
  (pageSize + 9) / 10
}

private func makeEven(_ n: Int) -> Int { n % 2 == 0 ? n : n + 1 }
