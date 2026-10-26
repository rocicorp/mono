import Testing
import ZeroTesting

@testable import ZeroVirtual

/// `RowsScrollView` without the platform view: each layout pass calls
/// `LayoutHost.layout` with the offset the user left it at, and draws the
/// frames and offset that come back. Query results arrive between passes, as
/// `ZeroVirtualizer.onSnapshot` delivers them.
@MainActor
final class LayoutScrollView {
  let host = LayoutHost()
  let h: Harness
  let viewportHeight: Double
  var heights: [String: Double] = [:]
  /// How many times each row was measured.
  var measures: [String: Int] = [:]
  /// The heights of the views above and below the rows.
  var header = 0.0
  var footer = 0.0
  private var offset = 0.0
  /// Passes that applied an offset other than the user's: the core
  /// scrolled, which can stop a fling.
  private(set) var scrolls = 0
  /// Each drawn row's top, from the viewport's top, as of the last pass.
  private var drawn: [AnyHashable: Double] = [:]
  private var unsubscribe: (() -> Void)?
  /// What the host asked to run at the start of the next frame.
  var nextFrame: [@MainActor () -> Void] = []
  /// The time: a frame takes 1/60 s, making a row 1 ms.
  var clock = 0.0

  init(_ h: Harness, viewportHeight: Double = 400) {
    self.h = h
    self.viewportHeight = viewportHeight
    host.rows = { [unowned self] in
      let s = self.h.core.snapshot
      return (s.items.map { (AnyHashable($0.key), $0.index) }, s.spaceBefore, s.spaceAfter)
    }
    host.measure = { [unowned self] key in
      let key = key.base as! String
      // Making a row's view takes the time; its height is kept.
      if self.measures[key] == nil { self.clock += 0.001 }
      self.measures[key, default: 0] += 1
      return self.heights[key] ?? self.h.view.rowHeight
    }
    host.estimatedRowHeight = { [unowned self] in self.h.core.options.estimatedRowHeight }
    host.header = { [unowned self] in self.header }
    host.footer = { [unowned self] in self.footer }
    host.didScroll = { [unowned self] in self.h.core.didScroll() }
    host.viewportDidResize = { [unowned self] in self.h.core.viewportDidResize() }
    host.afterUpdate = { [unowned self] in self.h.core.afterUpdate() }
    host.isLanding = { [unowned self] in self.h.core.isLanding }
    host.nextFrame = { [unowned self] in self.nextFrame.append($0) }
    host.now = { [unowned self] in self.clock }
    unsubscribe = h.core.subscribe { [unowned self] in self.host.invalidate() }
    h.core.attach(host)
  }

  var scrollOffset: Double { offset }

  /// The query's rows as they are now, then one layout pass.
  func pass() {
    h.core.setRows(h.answer(h.core.queryInputs))
    host.invalidate()
    let r = host.layout(offset: offset, viewport: viewportHeight)
    if r.offset != offset { scrolls += 1 }
    offset = r.offset
    drawn = [:]
    for f in r.frames { drawn[f.key] = f.minY - r.offset }
  }

  /// The next frame starts: runs what the host asked to run then; whether
  /// there was any.
  @discardableResult
  func frameDrawn() -> Bool {
    clock += 1.0 / 60
    let work = nextFrame
    nextFrame = []
    for w in work { w() }
    return !work.isEmpty
  }

  /// Passes, the frames drawn between them, until the query and the layout
  /// stop changing. With `holding`,
  /// that row, while drawn, is drawn in the same place on every pass.
  func settle(holding key: String? = nil, sourceLocation: SourceLocation = #_sourceLocation) {
    let top = key.flatMap(rowTop)
    for _ in 0..<30 {
      let before = (h.queryKey(h.core.queryInputs), offset, h.core.snapshot.spaceBefore)
      pass()
      if let key, let top, let now = rowTop(key) {
        #expect(now == top, "\(key) drawn moved", sourceLocation: sourceLocation)
      }
      if !frameDrawn(),
        (h.queryKey(h.core.queryInputs), offset, h.core.snapshot.spaceBefore) == before,
        h.core.snapshot.complete
      {
        return
      }
    }
    Issue.record("did not settle", sourceLocation: sourceLocation)
  }

  /// The user scrolls to `y`, within the content, and the view lays out.
  func userScroll(_ y: Double) {
    clock += 1.0 / 60
    offset = max(0, min(y, host.contentHeight - viewportHeight))
    pass()
  }

  /// Where the row was drawn on the last pass, from the viewport's top.
  func rowTop(_ key: String) -> Double? { drawn[AnyHashable(key)] }

  var bottomGap: Double { host.contentHeight - offset - viewportHeight }
}

@MainActor
@Suite("Layout host") struct LayoutHostTests {
  @Test("pages down and back up, the top row holding still as pages come and go")
  func pages() {
    let h = Harness(rowCount: 1000) { $0.anchoring = .manual }
    let view = LayoutScrollView(h)
    view.settle()
    #expect(h.core.snapshot.items.first?.key == "r0")

    for _ in 0..<40 {
      view.userScroll(view.scrollOffset + 300)
      view.settle(holding: h.core.firstVisibleItem()?.key)
    }
    #expect(view.rowTop("r600") != nil)

    for _ in 0..<40 {
      view.userScroll(view.scrollOffset - 300)
      view.settle(holding: h.core.firstVisibleItem()?.key)
    }
    #expect(h.core.snapshot.items.first?.key == "r0")
    #expect(view.scrollOffset == 0)
  }

  @Test("rows taller than estimated above the viewport don't move it")
  func dynamicHeights() {
    let h = Harness(rowCount: 300) { $0.anchoring = .manual }
    let view = LayoutScrollView(h)
    view.settle()
    view.userScroll(2000)
    view.settle()
    let key = h.core.firstVisibleItem()!.key
    let index = h.core.firstVisibleItem()!.index
    // Rows above it, rendered as overscan, turn out taller.
    for i in max(0, index - 4)..<index { view.heights["r\(i)"] = 75 }
    view.settle(holding: key)
  }

  @Test("at the start, rows shorter than estimated above take their space with them, not at rest")
  func shrinkageAtTheStart() {
    let h = Harness(rowCount: 300) { $0.anchoring = .manual }
    let view = LayoutScrollView(h)
    view.settle()
    view.userScroll(1000)
    view.settle()
    let key = h.core.firstVisibleItem()!.key
    let top = view.rowTop(key)
    view.heights["r30"] = 5
    view.settle(holding: key)
    // No space is left above the first row for the list to give back at
    // rest: a fling to the top would come to rest with a gap there.
    #expect(h.core.snapshot.spaceBefore == 0)
    #expect(view.rowTop(key) == top)
  }

  @Test(
    "a header changing height above the viewport doesn't move the rows; at the top it pushes them")
  func headerChangesHeight() {
    let h = Harness(rowCount: 300) { $0.anchoring = .manual }
    let view = LayoutScrollView(h)
    view.header = 100
    view.settle()
    #expect(view.rowTop("r0") == 100)
    view.userScroll(2000)
    view.settle()
    let key = h.core.firstVisibleItem()!.key
    let top = view.rowTop(key)
    for height in [250.0, 40, 0, 180] {
      view.header = height
      view.settle(holding: key)
    }
    h.scheduler.advance(by: 200)
    view.settle(holding: key)
    #expect(view.rowTop(key) == top)

    // At the top, the header is on screen: it grows, and the rows go down
    // with it.
    for _ in 0..<40 where view.scrollOffset > 0 {
      view.userScroll(view.scrollOffset - 300)
      view.settle()
    }
    #expect(view.scrollOffset == 0)
    view.header = 260
    view.settle()
    #expect(view.scrollOffset == 0)
    #expect(view.rowTop("r0") == 260)
  }

  @Test("scrolls to rows past the header and up to the footer")
  func scrollsAroundAHeaderAndAFooter() {
    let h = Harness(rowCount: 200) { $0.anchoring = .manual }
    let view = LayoutScrollView(h)
    view.header = 120
    view.footer = 80
    view.settle()
    #expect(view.rowTop("r0") == 120)

    h.core.scrollToItem("r199", align: .end)
    view.settle()
    // The last row's bottom at the viewport's, the footer below it.
    let last = try! #require(view.rowTop("r199"))
    #expect(last + (view.heights["r199"] ?? h.view.rowHeight) == view.viewportHeight)
    #expect(view.bottomGap == 80)

    h.core.scrollToItem("r0", align: .start)
    view.settle()
    #expect(view.rowTop("r0") == 0)
    #expect(view.scrollOffset == 120)

    h.core.scrollToItem("r100", align: .center)
    view.settle()
    let middle = try! #require(view.rowTop("r100"))
    #expect(abs(middle + h.view.rowHeight / 2 - view.viewportHeight / 2) <= 0.5)
  }

  @Test(
    "offline, a list opened at a permalink or where it was shows its rows once they're in place")
  func opensOffline() {
    // Where the list was, as it reported it.
    let online = Harness(rowCount: 300) { $0.anchoring = .manual }
    var saved: VirtualScrollState<TestRow, String>?
    online.setOptions { $0.onScrollStateChange = { saved = $0 } }
    let first = LayoutScrollView(online)
    first.settle()
    first.userScroll(2400)
    first.settle()
    let top = try! #require(online.core.firstVisibleItem()?.key)
    let state = try! #require(saved)

    let opens: [(key: String, open: (inout Harness.Core.Options) -> Void)] = [
      ("r150", { $0.permalinkID = "r150" }),
      (top, { $0.scrollState = state }),
    ]
    for (key, open) in opens {
      let h = Harness(rowCount: 300) {
        $0.anchoring = .manual
        open(&$0)
      }
      // No server: the queries never complete.
      h.offline = true
      let view = LayoutScrollView(h)
      for _ in 0..<10 { view.pass() }
      #expect(!h.core.snapshot.complete)
      // The view draws the rows only once the list isn't landing.
      #expect(!h.core.isLanding)
      #expect(h.core.firstVisibleItem()?.key == key)
    }
  }

  @Test("stuck to the bottom, it follows new rows and a last row growing taller")
  func sticks() {
    let h = Harness(rowCount: 60) {
      $0.anchoring = .manual
      $0.stickToBottom = true
    }
    let view = LayoutScrollView(h)
    view.settle()
    view.userScroll(.infinity)
    view.settle()
    #expect(view.bottomGap == 0)

    h.data += (60..<63).map { TestRow(id: "r\($0)") }
    view.settle()
    #expect(view.bottomGap == 0)
    #expect(h.core.lastVisibleItem()?.key == "r62")

    view.heights["r62"] = 160
    view.settle()
    #expect(view.bottomGap == 0)

    view.userScroll(view.scrollOffset - 300)
    let offset = view.scrollOffset
    h.data += (63..<66).map { TestRow(id: "r\($0)") }
    view.settle()
    #expect(view.scrollOffset == offset)
  }

  @Test("stuck to the bottom of a list opened at its last row, a scroll up moves the rows by it")
  func scrollUpFromTheBottom() {
    let h = Harness(rowCount: 400, rowHeight: 80) {
      $0.anchoring = .manual
      $0.permalinkID = "r399"
      $0.stickToBottom = true
      $0.minPageSize = 20
    }
    let view = LayoutScrollView(h)
    for i in 0..<400 { view.heights["r\(i)"] = [62.3, 180.6, 38.5, 121.0, 76.0][i % 5] }
    view.settle()
    h.scheduler.advance(by: 200)
    view.settle()
    #expect(view.bottomGap == 0)
    // The core's scrolls past the bottom stay where they are: the user's
    // next scroll is the user's, not taken back to the bottom.
    let top = try! #require(view.rowTop("r396"))
    view.userScroll(view.scrollOffset - 20.5)
    #expect(view.rowTop("r396") == top + 20.5)
  }

  @Test("scrolls to a row not loaded yet")
  func scrollToItemFarBelow() {
    let h = Harness(rowCount: 200) { $0.anchoring = .manual }
    let view = LayoutScrollView(h)
    view.settle()
    h.core.scrollToItem("r199", align: .end)
    view.settle()
    #expect(h.core.lastVisibleItem()?.key == "r199")
    #expect(view.bottomGap == 0)
  }

  @Test("a fling up through pages of taller rows moves the rows by the fling alone")
  func flingKeepsGoing() {
    let h = Harness(rowCount: 400) { $0.anchoring = .manual }
    let view = LayoutScrollView(h)
    for i in 0..<400 where i % 3 == 0 { view.heights["r\(i)"] = 60 }
    view.settle()
    h.core.scrollToItem("r300", align: .start)
    view.settle()
    h.scheduler.advance(by: 200)
    view.settle()

    // A fling: the offset moves every frame, and pages load under it.
    let scrolls = view.scrolls
    var key = h.core.firstVisibleItem()!.key
    for _ in 0..<60 where view.scrollOffset > 0 {
      let top = view.rowTop(key)!
      let offset = view.scrollOffset
      view.userScroll(offset - 40)
      // Each frame moves what's on screen by the fling's distance, no more,
      // through every pass the pages it loads take.
      #expect(view.rowTop(key) == top + (offset - view.scrollOffset))
      view.settle(holding: key)
      key = h.core.firstVisibleItem()!.key
    }
    // Rows that load or come out taller are taken up by the space before
    // them, not a scroll.
    #expect(view.scrolls == scrolls)

    // At rest, the space goes back to its estimate, nothing on screen moving.
    h.scheduler.advance(by: 200)
    view.settle(holding: key)
  }

  @Test(
    "opened at its end, flings up to the top through rows 3× their estimate, never drawn moved")
  func flingsUpFromAPermalink() {
    let h = Harness(rowCount: 400) {
      $0.anchoring = .manual
      $0.permalinkID = "r399"
    }
    let view = LayoutScrollView(h)
    for i in 0..<400 { view.heights["r\(i)"] = 60 }
    view.settle()
    h.scheduler.advance(by: 200)
    view.settle()
    #expect(h.core.lastVisibleItem()?.key == "r399")

    var flings = 0
    while view.scrollOffset > 0, flings < 200 {
      flings += 1
      let scrolls = view.scrolls
      // A fling of 8 coarse frames, pages loading under it. (Kept cheap: it
      // runs on the main actor alongside the other targets' tests.)
      for _ in 0..<8 where view.scrollOffset > 0 {
        guard let key = h.core.firstVisibleItem()?.key, let top = view.rowTop(key) else {
          Issue.record("no rows on screen")
          return
        }
        let offset = view.scrollOffset
        let target = max(0, offset - 100)
        view.userScroll(target)
        // What's on screen moves by the fling alone: near the start, rows
        // above measured taller than estimated are taken up by a scroll in
        // the same pass.
        #expect(view.rowTop(key) == top + (offset - target), "fling \(flings) at \(offset)")
        view.settle(holding: key)
        // At the top, no space stands above the first row.
        if view.scrollOffset == 0, h.core.snapshot.items.first?.key == "r0" {
          #expect(view.rowTop("r0") == 0, "a gap above r0 after fling \(flings)")
        }
      }
      // Away from the start, the space takes the pages up: no scroll under
      // the fling. (At the start, leftover space goes with a scroll in the
      // same pass.)
      if h.core.snapshot.items.first?.key != "r0" {
        #expect(view.scrolls == scrolls, "scrolled under fling \(flings)")
      }
      // At rest: the space is given back, nothing on screen moving.
      guard let key = h.core.firstVisibleItem()?.key else { return }
      h.scheduler.advance(by: 200)
      view.settle(holding: key)
    }
    #expect(h.core.snapshot.items.first?.key == "r0")
    #expect(view.scrollOffset == 0)
    #expect(h.core.snapshot.spaceBefore == 0)
  }

  @Test("only the rows near the viewport are measured")
  func measuresNearRows() {
    let h = Harness(rowCount: 1000) { $0.anchoring = .manual }
    let view = LayoutScrollView(h)
    view.settle()
    let items = h.core.snapshot.items
    #expect(items.count >= 100)
    // 20 rows on screen, and a viewport's height of them below.
    let measured = Set(view.measures.keys)
    #expect(measured == Set((0..<41).map { "r\($0)" }))
    #expect(view.host.rowFrames().count == items.count)
  }

  @Test("scrolling down measures the rows coming near, and the rows on screen move with the scroll")
  func measuresAsItScrolls() {
    let h = Harness(rowCount: 1000) { $0.anchoring = .manual }
    let view = LayoutScrollView(h)
    for i in 0..<1000 where i % 3 == 0 { view.heights["r\(i)"] = 60 }
    view.settle()
    for _ in 0..<30 {
      let key = h.core.firstVisibleItem()!.key
      let top = try! #require(view.rowTop(key))
      view.measures = [:]
      view.userScroll(view.scrollOffset + 150)
      #expect(view.rowTop(key) == top - 150)
      // What it measured is near the viewport, and takes in what's on it.
      let offset = view.scrollOffset
      for f in view.host.rowFrames() {
        let near = f.maxY >= offset - 400 && f.minY <= offset + 800
        let onScreen = f.maxY > offset && f.minY < offset + 400
        let measured = view.measures[f.key.base as! String] != nil
        #expect(!measured || near, "\(f.key) measured at \(f.minY), the viewport at \(offset)")
        #expect(!onScreen || measured, "\(f.key) on screen and not measured")
      }
      view.settle(holding: h.core.firstVisibleItem()?.key)
    }
  }

  @Test("rows above the viewport measured taller than estimated don't move what's on screen")
  func measuredTallerAbove() {
    // At the start, where the rows above are taken up by a scroll, and in
    // mid-list, where the space before the rows takes them up.
    for scrolled in [0.0, 6000] {
      let h = Harness(rowCount: 1000) { $0.anchoring = .manual }
      let view = LayoutScrollView(h)
      for i in 0..<1000 { view.heights["r\(i)"] = 60 }
      view.settle()
      for _ in 0..<40 where view.scrollOffset < scrolled {
        view.userScroll(view.scrollOffset + 300)
        view.settle()
      }
      // A jump over rows it doesn't measure.
      let first = h.core.firstVisibleItem()!.index
      let to = h.core.snapshot.items.last!.index - 10
      #expect(to - first > 45)
      h.core.scrollToRow(key: "r\(to)", align: .start)
      view.settle()
      #expect(view.rowTop("r\(to)") == 0)
      // Past the rows measured below where it was.
      let skipped = "r\(first + 16)"
      #expect(view.measures[skipped] == nil)

      // Scrolling up, the rows above come near and come out 3× their
      // estimate.
      for _ in 0..<60 where view.measures[skipped] == nil {
        let key = h.core.firstVisibleItem()!.key
        let top = try! #require(view.rowTop(key))
        let offset = view.scrollOffset
        view.userScroll(offset - 100)
        #expect(view.rowTop(key) == top + 100, "\(key) after a scroll from \(offset)")
        view.settle(holding: key)
      }
      #expect(view.measures[skipped] != nil)
    }
  }

  @Test("jumps to rows not measured yet land on them")
  func jumpsToUnmeasuredRows() {
    let alignments: [VirtualScrollAlignment] = [.start, .center, .end, .auto]
    for align in alignments {
      for byID in [false, true] {
        let h = Harness(rowCount: 1000) { $0.anchoring = .manual }
        let view = LayoutScrollView(h)
        for i in 0..<1000 { view.heights["r\(i)"] = i % 2 == 0 ? 60 : 35 }
        view.settle()
        #expect(view.measures["r80"] == nil)
        if byID {
          h.core.scrollToItem("r80", align: align)
        } else {
          h.core.scrollToRow(key: "r80", align: align)
        }
        view.settle()
        let top = try! #require(view.rowTop("r80"))
        let expected: Double =
          switch align {
          case .start: 0
          case .center: (view.viewportHeight - 60) / 2
          // Below the viewport, `.auto` lines its bottom up with the
          // viewport's.
          case .end, .auto: view.viewportHeight - 60
          }
        #expect(abs(top - expected) <= 1, "\(align) by id: \(byID)")
        h.scheduler.advance(by: 200)
        view.settle(holding: "r80")
      }
    }
  }
}

@MainActor
@Suite("Layout host, rows made over frames") struct LayoutHostBudgetTests {
  @Test(
    "a page's rows off screen are measured in later frames, the rows on screen in the pass that shows them"
  )
  func spreadsNewRows() {
    let h = Harness(rowCount: 1000) { $0.anchoring = .manual }
    let view = LayoutScrollView(h)
    for i in 0..<1000 { view.heights["r\(i)"] = i % 3 == 0 ? 50 : 30 }
    view.settle()
    for _ in 0..<40 {
      let key = h.core.firstVisibleItem()!.key
      let top = try! #require(view.rowTop(key))
      let known = Set(view.measures.keys)
      view.userScroll(view.scrollOffset + 150)
      #expect(view.rowTop(key) == top - 150)
      let offset = view.scrollOffset
      var newOff = 0
      for f in view.host.rowFrames() {
        let k = f.key.base as! String
        let onScreen = f.maxY > offset && f.minY < offset + 400
        if onScreen { #expect(view.measures[k] != nil, "\(k) on screen and not measured") }
        if !onScreen, view.measures[k] != nil, !known.contains(k) { newOff += 1 }
      }
      #expect(newOff == 0, "\(newOff) new rows off screen in the scroll's pass")
      // The frames after it measure the rest within reach, as many as the
      // frame's budget (4 ms, a row taking 1 ms) starts, nothing on screen
      // moving.
      let held = h.core.firstVisibleItem()!.key
      let heldTop = try! #require(view.rowTop(held))
      for _ in 0..<40 where view.frameDrawn() {
        let before = view.measures.count
        view.pass()
        let made = view.measures.count - before
        #expect(made <= 5, "\(made) rows in a frame")
        #expect(view.rowTop(held) == heldTop, "\(held) moved as rows off screen were measured")
      }
      for f in view.host.rowFrames() where f.maxY >= offset - 400 && f.minY <= offset + 800 {
        #expect(view.measures[f.key.base as! String] != nil, "\(f.key) near and never measured")
      }
    }
  }

  @Test("scrolling up, the new rows measured off screen are the ones just above it")
  func measuresNearestRowsFirst() {
    let h = Harness(rowCount: 1000) { $0.anchoring = .manual }
    let view = LayoutScrollView(h)
    for i in 0..<1000 { view.heights["r\(i)"] = i % 3 == 0 ? 50 : 30 }
    view.settle()
    for _ in 0..<30 {
      view.userScroll(view.scrollOffset + 300)
      view.settle()
    }
    for _ in 0..<10 {
      // Up by more than the reach above the screen: every row above the
      // screen, within reach, is new.
      view.measures = [:]
      view.userScroll(view.scrollOffset - 600)
      let offset = view.scrollOffset
      let above = view.host.rowFrames().filter { $0.maxY <= offset && $0.maxY >= offset - 400 }
        .map { (key: $0.key.base as! String, distance: offset - $0.maxY) }
      let made = above.filter { view.measures[$0.key] != nil }.map(\.distance)
      let waiting = above.filter { view.measures[$0.key] == nil }.map(\.distance)
      if let farthest = made.max(), let nearest = waiting.min() {
        #expect(farthest <= nearest, "a row \(farthest) above measured before one \(nearest) above")
      }
      view.settle()
    }
  }
}
