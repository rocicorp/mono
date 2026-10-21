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

  init(_ h: Harness, viewportHeight: Double = 400) {
    self.h = h
    self.viewportHeight = viewportHeight
    host.rows = { [unowned self] in
      let s = self.h.core.snapshot
      return (s.items.map { (AnyHashable($0.key), $0.index) }, s.spaceBefore, s.spaceAfter)
    }
    host.measure = { [unowned self] key in
      self.heights[key.base as! String] ?? self.h.view.rowHeight
    }
    host.header = { [unowned self] in self.header }
    host.footer = { [unowned self] in self.footer }
    host.didScroll = { [unowned self] in self.h.core.didScroll() }
    host.viewportDidResize = { [unowned self] in self.h.core.viewportDidResize() }
    host.afterUpdate = { [unowned self] in self.h.core.afterUpdate() }
    host.isLanding = { [unowned self] in self.h.core.isLanding }
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

  /// Passes until the query and the layout stop changing. With `holding`,
  /// that row, while drawn, is drawn in the same place on every pass.
  func settle(holding key: String? = nil, sourceLocation: SourceLocation = #_sourceLocation) {
    let top = key.flatMap(rowTop)
    for _ in 0..<30 {
      let before = (h.queryKey(h.core.queryInputs), offset, h.core.snapshot.spaceBefore)
      pass()
      if let key, let top, let now = rowTop(key) {
        #expect(now == top, "\(key) drawn moved", sourceLocation: sourceLocation)
      }
      if (h.queryKey(h.core.queryInputs), offset, h.core.snapshot.spaceBefore) == before,
        h.core.snapshot.complete
      {
        return
      }
    }
    Issue.record("did not settle", sourceLocation: sourceLocation)
  }

  /// The user scrolls to `y`, within the content, and the view lays out.
  func userScroll(_ y: Double) {
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
        view.userScroll(offset - 100)
        #expect(
          view.rowTop(key) == top + (offset - view.scrollOffset), "fling \(flings) at \(offset)")
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
}
