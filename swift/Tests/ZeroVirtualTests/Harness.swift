import Testing
import ZeroTesting

@testable import ZeroVirtual

/// A row of the harness's list: `r0`, `r1`, …
struct TestRow: Equatable, Sendable, Codable {
  var id: String
}

/// The scroll view of the harness (zero-virtual's `createScroller` and the
/// DOM it lays out): rows in a column under `spaceBefore`, over
/// `spaceAfter`. The offset clamps to the content like a browser's, and a
/// change is reported when the test delivers it, as a browser's scroll event
/// comes after the write; with `synchronousEvents`, during the write, as
/// `UIScrollView` reports it.
@MainActor
final class FakeScrollView: VirtualHost {
  var viewportHeight: Double
  var viewportInsets: (top: Double, bottom: Double) = (0, 0)
  var synchronousEvents = false
  var scrollsWithLayout = false
  weak var core: VirtualizerCore<String, TestRow, String, TestRow>?

  private(set) var items: [VirtualItem<TestRow, String>] = []
  private var spaceBefore = 0.0
  var renderedSpaceBefore: Double { spaceBefore }
  private var spaceAfter = 0.0
  /// Space above the rows that isn't theirs: other content over the list.
  var marginTop = 0.0
  private var offset = 0.0
  private var pending = false
  var heights: [String: Double] = [:]
  let rowHeight: Double

  init(viewportHeight: Double, rowHeight: Double) {
    self.viewportHeight = viewportHeight
    self.rowHeight = rowHeight
  }

  func render(_ snapshot: VirtualSnapshot<TestRow, String>) {
    items = snapshot.items
    spaceBefore = snapshot.spaceBefore
    spaceAfter = snapshot.spaceAfter
  }

  private func height(_ key: String) -> Double { heights[key] ?? rowHeight }

  var contentHeight: Double {
    marginTop + spaceBefore + items.reduce(0) { $0 + height($1.key) } + spaceAfter
  }

  private var maxOffset: Double { max(0, contentHeight - viewportHeight) }

  var scrollOffset: Double {
    if offset > maxOffset {
      offset = maxOffset
      pending = true
    }
    return offset
  }

  func scroll(to target: Double) {
    let next = max(0, min(target, maxOffset))
    guard next != offset else { return }
    offset = next
    pending = true
    if synchronousEvents { deliverScroll() }
  }

  func rowFrames() -> [VirtualRowFrame] {
    var y = marginTop + spaceBefore
    return items.map { item in
      let h = height(item.key)
      defer { y += h }
      return VirtualRowFrame(index: item.index, key: AnyHashable(item.key), minY: y, maxY: y + h)
    }
  }

  func deliverScroll() {
    if pending {
      pending = false
      core?.didScroll()
    }
  }

  func userScroll(_ top: Double) {
    let next = max(0, min(top, maxOffset))
    if next != offset {
      offset = next
      pending = true
    }
    deliverScroll()
  }
}

/// zero-virtual's DOM test harness (`virtualizer.dom.test.ts`): the core
/// driving a fake scroll view, with queries answered from an in-memory list
/// through the real `assembleRows`.
///
/// `tick()` plays one render: answer the query inputs, render the snapshot,
/// run `afterUpdate`, then deliver any scroll the update wrote. `settle()`
/// ticks until the queries stop changing.
@MainActor
final class Harness {
  typealias Core = VirtualizerCore<String, TestRow, String, TestRow>

  var data: [TestRow]
  let view: FakeScrollView
  let scheduler = ManualScheduler()
  let core: Core
  var options: Core.Options
  private var partial: (skip: Int, from: String, key: String?)?
  /// Every answer is incomplete, as Zero answers from the client's store
  /// with no server.
  var offline = false
  private var permalinkBeforePending = false

  init(
    rowCount: Int, rowHeight: Double = 20, viewportHeight: Double = 400,
    configure: (inout Core.Options) -> Void = { _ in }
  ) {
    data = (0..<rowCount).map { TestRow(id: "r\($0)") }
    view = FakeScrollView(viewportHeight: viewportHeight, rowHeight: rowHeight)
    var options = Core.Options(
      estimatedRowHeight: rowHeight, rowKey: \.id, toStartRow: { $0 }, listContext: "ctx")
    options.anchoring = .native
    // A 100-row page, which the expectations count in.
    options.minPageSize = 100
    configure(&options)
    self.options = options
    core = Core(options, scheduler: scheduler)
    view.core = core
  }

  func setOptions(_ change: (inout Core.Options) -> Void) {
    change(&options)
    core.setOptions(options)
  }

  /// Zero-demo paging: an exclusive start, backward pages closest first.
  func page(_ start: TestRow?, _ direction: VirtualDirection, _ limit: Int) -> [TestRow] {
    let startIndex =
      start.flatMap { s in data.firstIndex { $0.id == s.id } }
      ?? (direction == .forward ? -1 : data.count)
    switch direction {
    case .forward:
      return Array(data[min(data.count, startIndex + 1)..<min(data.count, startIndex + 1 + limit)])
    case .backward:
      return Array(data[max(0, startIndex - limit)..<startIndex].reversed())
    }
  }

  static func pageKey(_ anchor: VirtualAnchor<TestRow>) -> String {
    switch anchor {
    case .permalink(_, let id): "permalink:\(id)"
    case .forward(_, let s): "forward:\(s?.id ?? "")"
    case .backward(_, let s): "backward:\(s.id)"
    }
  }

  func queryKey(_ i: RowsQueryInputs<TestRow>) -> String {
    "\(Self.pageKey(i.anchor)):\(i.anchor.index):\(i.pageSize):\(i.probeID ?? "")"
  }

  func answer(_ inputs: RowsQueryInputs<TestRow>) -> RowsSnapshot<TestRow, TestRow> {
    if case .permalink(_, let id) = inputs.anchor {
      let single = data.first { $0.id == id }
      let beforePending = permalinkBeforePending
      permalinkBeforePending = false
      return assembleRows(
        inputs,
        RowsQueryResults(
          singleRow: single, singleComplete: !offline,
          mainRows: single.map {
            beforePending ? [] : page($0, .backward, inputs.pageSize / 2 + 1)
          },
          mainComplete: !beforePending && !offline,
          afterRows: single.map { page($0, .forward, inputs.pageSize / 2) },
          afterComplete: !offline))
    }
    let probe = inputs.probeID.flatMap { id in data.first { $0.id == id } }
    let key = Self.pageKey(inputs.anchor)
    let limit = inputs.pageSize + 1
    let (start, direction): (TestRow?, VirtualDirection) =
      switch inputs.anchor {
      case .forward(_, let s): (s, .forward)
      case .backward(_, let s): (s, .backward)
      case .permalink: (nil, .forward)
      }
    var main = page(start, direction, limit)
    var complete = !offline
    if var p = partial, key != p.from, (p.key ?? key) == key {
      if p.key == nil {
        p.key = key
        partial = p
        main = Array(page(start, direction, limit + p.skip).dropFirst(p.skip))
        complete = false
      } else {
        partial = nil
      }
    }
    return assembleRows(
      inputs,
      RowsQueryResults(
        singleRow: probe, singleComplete: inputs.probeID != nil, mainRows: main,
        mainComplete: complete))
  }

  func tick(scroll: Bool = true) {
    core.setRows(answer(core.queryInputs))
    view.render(core.snapshot)
    core.attach(view)
    core.afterUpdate()
    // A new space before the rows (taking up a change above them) renders
    // again at once, as the view re-renders on every snapshot.
    for _ in 0..<3 where core.snapshot.spaceBefore != view.renderedSpaceBefore {
      view.render(core.snapshot)
      core.afterUpdate()
    }
    if scroll { view.deliverScroll() }
  }

  /// A render before the scroll view is there.
  func tickDetached() {
    core.setRows(answer(core.queryInputs))
    core.detach()
    core.afterUpdate()
  }

  func settle(maxTicks: Int = 20, sourceLocation: SourceLocation = #_sourceLocation) {
    for _ in 0..<maxTicks {
      let before = queryKey(core.queryInputs)
      tick()
      if queryKey(core.queryInputs) == before && core.snapshot.complete { return }
    }
    Issue.record("paging did not settle", sourceLocation: sourceLocation)
  }

  func userScroll(_ top: Double) { view.userScroll(top) }

  var scrollOffset: Double { view.scrollOffset }

  var items: [VirtualItem<TestRow, String>] { core.snapshot.items }

  var visibleIndexes: [Int] {
    let top = view.scrollOffset
    return view.rowFrames().filter { $0.maxY > top && $0.minY < top + view.viewportHeight }
      .map(\.index)
  }

  /// Where the row is from the viewport's top.
  func rowTop(_ key: String, sourceLocation: SourceLocation = #_sourceLocation) -> Double {
    guard let f = view.rowFrames().first(where: { $0.key == AnyHashable(key) }) else {
      Issue.record("row \(key) not rendered", sourceLocation: sourceLocation)
      return .nan
    }
    return f.minY - view.scrollOffset
  }

  func setRowHeight(_ key: String, _ height: Double) { view.heights[key] = height }

  /// Answers the next new page query partially once: missing its first
  /// `skip` rows and incomplete, as Zero answers from what the client has.
  func partialNext(_ skip: Int) {
    partial = (skip, Self.pageKey(core.queryInputs.anchor), nil)
  }

  /// Answers the next permalink query without the rows before its target.
  func partialPermalinkNext() { permalinkBeforePending = true }
}

/// `start...end` as an array.
func range(_ start: Int, _ end: Int) -> [Int] { Array(start...end) }
