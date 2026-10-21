import Foundation
import Testing
import ZeroTesting

@testable import ZeroVirtual

/// zero-virtual's `src/core/virtualizer.dom.test.ts`: the core driving a
/// scroll view whose geometry the test controls. Names are upstream's.
@MainActor
@Suite("paging against a real (fake-geometry) scroll container") struct PagingTests {
  @Test("initial load anchors at the top and fills one page")
  func initialLoad() {
    let h = Harness(rowCount: 500)
    h.settle()
    let s = h.core.snapshot
    #expect(s.items.first?.index == 0)
    #expect(s.items.count == 100)
    #expect(s.items.first?.row == TestRow(id: "r0"))
    #expect(s.spaceBefore == 0)
    #expect(h.visibleIndexes == range(0, 19))
  }

  @Test("scrolling near the window end advances the forward anchor")
  func pagesForward() {
    let h = Harness(rowCount: 500)
    h.settle()
    h.userScroll(1500)
    h.settle()
    let s = h.core.snapshot
    #expect(s.items.first?.index == 56)
    #expect(s.items.count == 100)
    #expect(s.spaceBefore == 56 * 20)
    for item in s.items { #expect(item.row.id == "r\(item.index)") }
    #expect(h.visibleIndexes == range(75, 94))
  }

  @Test("scrolling back near the window start pages backward")
  func pagesBackward() {
    let h = Harness(rowCount: 500)
    h.settle()
    h.userScroll(1500)
    h.settle()
    #expect(h.items.first?.index == 56)
    h.userScroll(1140)
    let visibleBefore = h.visibleIndexes
    #expect(visibleBefore == range(57, 76))
    h.settle()
    let s = h.core.snapshot
    #expect(s.items.first?.index == 0)
    #expect(s.items.count == 96)
    #expect(s.spaceBefore == 0)
    for item in s.items { #expect(item.row.id == "r\(item.index)") }
    #expect(h.visibleIndexes == visibleBefore)
  }

  /// A list whose every row has been seen, back at the top: what's below
  /// the loaded page is space standing in for rows seen before. Upstream
  /// passes `count: 500` for that space instead.
  func seenToTheEnd() -> Harness {
    let h = Harness(rowCount: 500)
    h.settle()
    while h.items.last?.index != 499 {
      h.userScroll(h.scrollOffset + 1500)
      h.settle()
    }
    h.userScroll(0)
    h.settle()
    #expect(h.core.snapshot.spaceAfter == (500 - 100) * 20)
    return h
  }

  @Test("a far jump into unloaded space below recovers by cascading pages")
  func farJumpDown() {
    let h = seenToTheEnd()
    h.userScroll(6000)
    h.settle()
    #expect(h.visibleIndexes == range(300, 319))
    let s = h.core.snapshot
    #expect(s.items.first?.index == 239)
    #expect(s.items.count == 100)
    #expect(s.spaceBefore == 239 * 20)
    #expect(h.scrollOffset == 6000)
    for item in s.items { #expect(item.row.id == "r\(item.index)") }
  }

  @Test("a far jump back to the very top re-anchors at the start")
  func farJumpToTop() {
    let h = seenToTheEnd()
    h.userScroll(6000)
    h.settle()
    #expect(h.items.first?.index == 239)
    h.userScroll(0)
    h.settle()
    let s = h.core.snapshot
    #expect(s.items.first?.index == 0)
    #expect(s.items.count == 100)
    #expect(s.spaceBefore == 0)
    #expect(h.visibleIndexes == range(0, 19))
  }

  @Test("a page too small for what is on screen grows instead of paging back and forth")
  func pageGrows() {
    let h = Harness(rowCount: 500) {
      $0.estimatedRowHeight = 200
      $0.minPageSize = 10
    }
    h.settle()
    #expect(h.core.queryInputs.pageSize >= 30)
    let visible = h.visibleIndexes
    #expect(visible == range(0, 19))
    #expect(h.items.last!.index > visible.last! + 5)
  }

  @Test("paging works the same when the scroll view reports scrolls as they happen")
  func synchronousScrollEvents() {
    let h = Harness(rowCount: 500)
    h.view.synchronousEvents = true
    h.settle()
    h.userScroll(1500)
    h.settle()
    #expect(h.items.first?.index == 56)
    #expect(h.visibleIndexes == range(75, 94))
    h.userScroll(1140)
    h.settle()
    #expect(h.items.first?.index == 0)
    #expect(h.visibleIndexes == range(57, 76))
  }
}

@MainActor
@Suite("manual scroll anchoring against a real (fake-geometry) container")
struct ManualAnchoringTests {
  func scrolled() -> Harness {
    let h = Harness(rowCount: 500) { $0.anchoring = .manual }
    h.settle()
    h.userScroll(1000)
    h.settle()
    return h
  }

  @Test("growth above the anchor row is compensated into scrollTop")
  func growthAbove() {
    let h = scrolled()
    #expect(h.rowTop("r50") == 0)
    h.setRowHeight("r30", 50)
    h.tick()
    #expect(h.scrollOffset == 1030)
    #expect(h.rowTop("r50") == 0)
  }

  @Test("shrinkage above the anchor row is taken up by space at the start, given back at rest")
  func shrinkageAbove() {
    let h = scrolled()
    h.setRowHeight("r30", 5)
    h.tick()
    // No scroll while the list may be moving: a scroll would stop a fling.
    #expect(h.scrollOffset == 1000)
    #expect(h.core.snapshot.spaceBefore == 15)
    #expect(h.rowTop("r50") == 0)

    h.scheduler.advance(by: 150)
    h.tick()
    #expect(h.core.snapshot.spaceBefore == 0)
    #expect(h.scrollOffset == 985)
    #expect(h.rowTop("r50") == 0)
  }

  @Test(
    "on a host that scrolls with its layout, shrinkage at the start comes out with a scroll at once"
  )
  func shrinkageAboveScrollsWithLayout() {
    let h = scrolled()
    h.view.scrollsWithLayout = true
    h.setRowHeight("r30", 5)
    h.tick()
    #expect(h.core.snapshot.spaceBefore == 0)
    #expect(h.scrollOffset == 985)
    #expect(h.rowTop("r50") == 0)
  }

  @Test("away from the start, the space before the rows takes up growth; at rest it's given back")
  func spaceTakesUpChange() {
    let h = Harness(rowCount: 1000) { $0.anchoring = .manual }
    h.settle()
    for _ in 0..<12 {
      h.userScroll(h.scrollOffset + 400)
      h.settle()
    }
    let first = h.items.first!.index
    #expect(first > 0)
    let space = h.core.snapshot.spaceBefore
    let offset = h.scrollOffset
    let key = h.core.firstVisibleItem()!.key
    let top = h.rowTop(key)
    h.setRowHeight("r\(h.core.firstVisibleItem()!.index - 2)", 50)
    h.tick()
    // No scroll while the list may be moving: a scroll would stop a fling.
    #expect(h.scrollOffset == offset)
    #expect(h.core.snapshot.spaceBefore == space - 30)
    #expect(h.rowTop(key) == top)

    h.scheduler.advance(by: 150)
    h.tick()
    #expect(h.core.snapshot.spaceBefore == space)
    #expect(h.scrollOffset == offset + 30)
    #expect(h.rowTop(key) == top)
  }

  @Test("growth at scroll offset 0 is revealed, not compensated away")
  func growthAtTop() {
    let h = Harness(rowCount: 500) { $0.anchoring = .manual }
    h.settle()
    h.setRowHeight("r0", 60)
    h.tick()
    #expect(h.scrollOffset == 0)
  }

  @Test("growth below the anchor row does not move the viewport")
  func growthBelow() {
    let h = scrolled()
    h.setRowHeight("r80", 200)
    h.tick()
    #expect(h.scrollOffset == 1000)
    #expect(h.rowTop("r50") == 0)
  }

  @Test("rows of different heights page down and back without moving what is on screen")
  func dynamicHeights() {
    // Every row its own height, none the estimate: the space standing in for
    // unloaded rows is wrong, and paging must not move the rows on screen.
    let h = Harness(rowCount: 1000) { $0.anchoring = .manual }
    for i in 0..<1000 { h.setRowHeight("r\(i)", Double(12 + (i * 7) % 40)) }
    h.settle()
    for step in [900.0, 900, 900, 900, -700, -700, -700] {
      h.userScroll(h.scrollOffset + step)
      let onScreen = h.visibleIndexes.map { ("r\($0)", h.rowTop("r\($0)")) }
      h.settle()
      for (key, top) in onScreen { #expect(h.rowTop(key) == top, "\(key)") }
      for item in h.items { #expect(item.row.id == "r\(item.index)") }
    }
  }
}

@MainActor
@Suite("scroll-state restore when the container mounts lazily") struct LazyRestoreTests {
  static func saved(_ offset: Double) -> VirtualScrollState<TestRow, String> {
    VirtualScrollState(
      anchor: .forward(index: 0, startRow: nil), scrollOffset: offset, estimatedTotal: 100,
      hasReachedStart: true, hasReachedEnd: false, listContext: "ctx")
  }

  @Test("restores the saved scrollTop when the element attaches on a later commit")
  func restoresAfterAttach() {
    let h = Harness(rowCount: 100) { $0.scrollState = Self.saved(300) }
    h.tickDetached()
    h.tickDetached()
    #expect(h.scrollOffset == 0)
    h.settle()
    #expect(h.scrollOffset == 300)
  }

  @Test("does not persist a spurious scrollTop:0 while detached, clobbering the saved position")
  func noSpuriousPersist() {
    var persisted: [VirtualScrollState<TestRow, String>] = []
    let h = Harness(rowCount: 100) {
      $0.scrollState = Self.saved(300)
      $0.onScrollStateChange = { persisted.append($0) }
    }
    h.tickDetached()
    h.tickDetached()
    #expect(persisted.isEmpty)
    h.settle()
    #expect(!persisted.isEmpty)
    #expect(!persisted.map(\.scrollOffset).contains(0))
    #expect(h.scrollOffset == 300)
  }
}

@MainActor
@Suite("reporting the scroll state") struct ReportingTests {
  @Test("every scroll is reported as it happens, not once the scroll ends")
  func everyScroll() {
    var reported: [Double] = []
    let h = Harness(rowCount: 500) { $0.onScrollStateChange = { reported.append($0.scrollOffset) } }
    h.settle()
    h.userScroll(400)
    #expect(reported.last == 400)
    h.userScroll(900)
    #expect(reported.last == 900)
  }
}

@MainActor
@Suite("permalink scroll") struct PermalinkTests {
  @Test("scrolls the permalink row into view when its row key differs from the permalink id")
  func keyDiffersFromID() {
    let h = Harness(rowCount: 300) {
      $0.permalinkID = "r120"
      $0.rowKey = { "key-\($0.id)" }
    }
    h.settle()
    let top = h.rowTop("key-r120")
    #expect(top >= 0 && top < 40)
  }

  @Test(
    "%s anchoring: the row stays on top when the page grows under it",
    arguments: [VirtualAnchoring.manual, .native])
  func pageGrowsUnderPermalink(anchoring: VirtualAnchoring) {
    let h = Harness(rowCount: 1000, viewportHeight: 800) {
      $0.permalinkID = "r500"
      $0.anchoring = anchoring
    }
    h.settle()
    #expect(h.core.queryInputs.pageSize == 120)
    #expect(h.rowTop("r500") == (anchoring == .manual ? 0 : 20))
  }

  @Test("the jump holds on while the rows before its target are still on their way")
  func holdsForRowsBefore() {
    let h = Harness(rowCount: 1000) {
      $0.permalinkID = "r500"
      $0.anchoring = .manual
    }
    h.view.viewportInsets = (20, 0)
    h.partialPermalinkNext()
    h.settle()
    #expect(h.rowTop("r500") == 20)
  }
}

@MainActor
@Suite("scroll-state restore of a position we wrote") struct WrittenRestoreTests {
  @Test("a later navigation back to it restores it")
  func navigationBack() {
    var persisted: [VirtualScrollState<TestRow, String>] = []
    let h = Harness(rowCount: 500) { $0.onScrollStateChange = { persisted.append($0) } }
    h.settle()
    h.userScroll(400)
    let earlier = persisted.last!
    #expect(earlier.scrollOffset == 400)
    h.userScroll(1200)
    h.settle()
    h.core.restore(earlier)
    h.tick()
    #expect(h.scrollOffset == 400)

    // And back to it a second time, an equal state.
    h.userScroll(1200)
    h.settle()
    h.core.restore(earlier)
    h.tick()
    #expect(h.scrollOffset == 400)
  }

  /// Runs `steps` against a host that keeps the state to itself or, with
  /// `echo`, hands every write straight back, as zbugs' router does; and
  /// returns where the list was after each.
  func run(echo: Bool, _ steps: (Harness, () -> Void) -> Void) -> [String] {
    var written: VirtualScrollState<TestRow, String>?
    let h = Harness(rowCount: 1000) {
      $0.onScrollStateChange = { if echo { written = $0 } }
    }
    func rerender() {
      for _ in 0..<10 {
        guard let state = written else { return }
        written = nil
        h.core.restore(state)
        h.tick()
      }
      #expect(written == nil, "each write echoes another")
    }
    var trace: [String] = []
    func settle() {
      h.settle()
      rerender()
      h.settle()
      trace.append("\(h.scrollOffset) \(h.visibleIndexes)")
    }
    settle()
    steps(h) {
      rerender()
      settle()
    }
    return trace
  }

  @Test(
    "from a host that hands every write straight back: scrolling: down several pages and back up")
  func echoScrolling() {
    let steps: (Harness, () -> Void) -> Void = { h, settle in
      for step in [600.0, 600, 600, 600, 600, -900, -900] {
        h.userScroll(h.scrollOffset + step)
        settle()
      }
    }
    #expect(run(echo: true, steps) == run(echo: false, steps))
  }

  @Test("from a host that hands every write straight back: a jump")
  func echoJump() {
    let steps: (Harness, () -> Void) -> Void = { h, settle in
      h.userScroll(1500)
      settle()
      h.core.scrollToItem("r900", align: .start)
      settle()
    }
    #expect(run(echo: true, steps) == run(echo: false, steps))
  }
}

@MainActor
@Suite("a restore that moves the loaded window") struct MovingRestoreTests {
  func scrolledAwayFromSaved(
    _ anchoring: VirtualAnchoring
  ) -> (Harness, VirtualScrollState<TestRow, String>) {
    var persisted: [VirtualScrollState<TestRow, String>] = []
    let h = Harness(rowCount: 500) {
      $0.anchoring = anchoring
      $0.onScrollStateChange = { persisted.append($0) }
    }
    h.settle()
    h.userScroll(600)
    h.settle()
    let saved = persisted.last!
    #expect(saved.scrollOffset == 600)
    for offset in [1600.0, 2600, 3600, 4600] {
      h.userScroll(offset)
      h.settle()
    }
    #expect(h.items.first!.index > 30)
    return (h, saved)
  }

  @Test(
    "manual anchoring lands it where it was saved (first answer missing %i rows)",
    arguments: [0, 30])
  func manualLands(skip: Int) {
    let (h, saved) = scrolledAwayFromSaved(.manual)
    h.partialNext(skip)
    h.core.restore(saved)
    h.tick(scroll: false)
    h.tick(scroll: false)
    h.view.deliverScroll()
    h.settle()
    #expect(h.scrollOffset == 600)
    #expect(h.rowTop("r30") == 0)
  }

  @Test(
    "%s anchoring: a position the outgoing window was too short to reach still lands",
    arguments: [VirtualAnchoring.manual, .native])
  func shortWindowLands(anchoring: VirtualAnchoring) {
    let (far, _) = scrolledAwayFromSaved(anchoring)
    var persisted: [VirtualScrollState<TestRow, String>] = []
    far.setOptions { $0.onScrollStateChange = { persisted.append($0) } }
    far.userScroll(4600)
    far.settle()
    let saved = persisted.last!
    let firstRow = Int(saved.scrollOffset / 20)

    let h = Harness(rowCount: 500) { $0.anchoring = anchoring }
    h.settle()
    h.core.restore(saved)
    h.settle()
    #expect(h.scrollOffset == saved.scrollOffset)
    #expect(h.rowTop("r\(firstRow)") == 0)
  }

  @Test(
    "%s anchoring: paging leaves the restored window alone until its rows are in",
    arguments: [VirtualAnchoring.manual, .native])
  func pagingWaits(anchoring: VirtualAnchoring) {
    let (h, saved) = scrolledAwayFromSaved(anchoring)
    h.core.restore(saved)
    h.tick()
    #expect(h.core.queryInputs.anchor == saved.anchor)
    h.settle()
    #expect(h.scrollOffset == 600)
    #expect(h.rowTop("r30") == 0)
  }
}

@MainActor
@Suite("compareStartRows") struct StartRowTests {
  struct Cursor: Equatable, Sendable, Codable { var id: String }

  @Test("or narrow the start row in toStartRow: the anchor carries what it returns")
  func narrowStartRow() throws {
    // Start rows are `Equatable`, so any type compares; a narrower one than
    // the row still has to survive saving, which is `Codable` here.
    typealias Core = VirtualizerCore<String, TestRow, String, Cursor>
    var persisted: [VirtualScrollState<Cursor, String>] = []
    var options = Core.Options(
      estimatedRowHeight: 20, rowKey: \.id, toStartRow: { Cursor(id: $0.id) }, listContext: "ctx")
    options.minPageSize = 100
    options.onScrollStateChange = { persisted.append($0) }
    let core = Core(options, scheduler: ManualScheduler())
    let data = (0..<500).map { TestRow(id: "r\($0)") }
    let view = CursorScrollView(core: core, data: data)
    view.settle()
    view.scroll(by: 1500)
    view.settle()

    guard case .forward(_, let start?) = core.queryInputs.anchor else {
      Issue.record("expected a page anchor")
      return
    }
    #expect(start.id.hasPrefix("r"))
    let saved = try #require(persisted.last)
    #expect(saved.anchor == core.queryInputs.anchor)
    let restored = try JSONDecoder().decode(
      VirtualScrollState<Cursor, String>.self, from: JSONEncoder().encode(saved))
    #expect(restored == saved)
  }
}

@MainActor
@Suite("firstVisibleItem / lastVisibleItem") struct VisibleItemTests {
  @Test("report the edges of the viewport, not of the loaded window")
  func edges() {
    let h = Harness(rowCount: 500)
    h.settle()
    #expect(h.core.firstVisibleItem()?.index == 0)
    #expect(h.core.lastVisibleItem()?.index == 19)
    #expect(h.items.count > 20)
  }

  @Test("follow the scroll position")
  func follow() {
    let h = Harness(rowCount: 500)
    h.settle()
    h.userScroll(500)
    h.tick()
    #expect(h.core.firstVisibleItem()?.index == 25)
    #expect(h.core.lastVisibleItem()?.index == 44)
  }

  @Test("count a row that overlaps the scrollport at all")
  func overlap() {
    let h = Harness(rowCount: 500)
    h.settle()
    h.userScroll(510)
    h.tick()
    #expect(h.core.firstVisibleItem()?.index == 25)
  }

  @Test("carry the row and its key, which scrollToItem takes")
  func carryRowAndKey() {
    let h = Harness(rowCount: 500)
    h.settle()
    h.userScroll(500)
    h.tick()
    let first = h.core.firstVisibleItem()!
    #expect(first.row == TestRow(id: "r25"))
    h.core.scrollToRow(key: first.key, align: .start)
    #expect(h.rowTop("r25") == 0)
  }

  @Test("are undefined with no scroll element attached")
  func detached() {
    let h = Harness(rowCount: 500)
    h.settle()
    h.core.detach()
    #expect(h.core.firstVisibleItem() == nil)
    #expect(h.core.lastVisibleItem() == nil)
  }

  @Test("follow the viewport's size")
  func viewportResize() {
    let h = Harness(rowCount: 500)
    h.settle()
    #expect(h.core.lastVisibleItem()?.key == "r19")
    h.view.viewportHeight = 200
    h.core.viewportDidResize()
    #expect(h.core.lastVisibleItem()?.key == "r9")
    h.view.viewportHeight = 0
    h.core.viewportDidResize()
    #expect(h.core.firstVisibleItem() == nil)
  }
}

@MainActor
@Suite("no-op re-anchor does not spin the render loop") struct NoOpReanchorTests {
  @Test(
    "loaded rows entirely below the viewport at scroll-top stay put (window list under other page content)"
  )
  func rowsBelowViewport() {
    let h = Harness(rowCount: 5)
    h.settle()
    #expect(h.core.snapshot.complete)
    h.view.marginTop = 1000
    var notified = 0
    let unsubscribe = h.core.subscribe { notified += 1 }
    h.core.afterUpdate()
    h.core.afterUpdate()
    h.core.afterUpdate()
    unsubscribe()
    #expect(notified == 0)
  }
}

@MainActor
@Suite("below-viewport window at the start of the list") struct BelowViewportTests {
  @Test("scrolling while the loaded rows sit below the viewport does not empty them")
  func doesNotEmpty() {
    let h = Harness(rowCount: 300)
    h.settle()
    #expect(!h.items.isEmpty)
    h.view.marginTop = 1000
    h.userScroll(150)
    h.settle()
    #expect(!h.items.isEmpty)
  }
}

@MainActor
@Suite("scrollToItem") struct ScrollToItemTests {
  @Test("a loaded row scrolls immediately, without re-anchoring")
  func loadedRow() {
    let h = Harness(rowCount: 500)
    h.settle()
    let anchorBefore = h.core.queryInputs.anchor
    h.core.scrollToItem("r50", align: .start)
    #expect(h.rowTop("r50") == 0)
    #expect(h.core.queryInputs.anchor == anchorBefore)
  }

  @Test("an unloaded row re-anchors and the scroll lands once its page renders")
  func unloadedRow() {
    let h = Harness(rowCount: 500) { $0.anchoring = .manual }
    h.settle()
    h.core.scrollToItem("r400", align: .start)
    h.settle()
    #expect(h.rowTop("r400") == 0)
  }

  @Test("is level-triggered: the same id twice scrolls twice")
  func levelTriggered() {
    let h = Harness(rowCount: 500)
    h.settle()
    h.core.scrollToItem("r50", align: .start)
    #expect(h.rowTop("r50") == 0)
    h.userScroll(0)
    h.tick()
    #expect(h.rowTop("r50") == 1000)
    h.core.scrollToItem("r50", align: .start)
    #expect(h.rowTop("r50") == 0)
  }

  @Test("align: defaults to auto: a row below the viewport scrolls just into view")
  func alignAuto() {
    let h = Harness(rowCount: 500)
    h.settle()
    h.core.scrollToItem("r50")
    #expect(h.rowTop("r50") == 380)
  }

  @Test("align: auto leaves an already-visible row where it is")
  func alignAutoVisible() {
    let h = Harness(rowCount: 500)
    h.settle()
    h.userScroll(500)
    h.settle()
    let before = h.scrollOffset
    h.core.scrollToItem("r30", align: .auto)
    #expect(h.scrollOffset == before)
  }

  @Test(
    "align: start, center and end",
    arguments: [(VirtualScrollAlignment.start, 0.0), (.center, 190), (.end, 380)])
  func align(align: VirtualScrollAlignment, top: Double) {
    let h = Harness(rowCount: 500)
    h.settle()
    h.core.scrollToItem("r50", align: align)
    #expect(h.rowTop("r50") == top)
  }

  @Test("aligns below the container's scroll-padding (a sticky header)")
  func insets() {
    let h = Harness(rowCount: 500)
    h.settle()
    h.view.viewportInsets = (60, 0)
    h.core.scrollToItem("r50", align: .start)
    #expect(h.rowTop("r50") == 60)
  }

  @Test("alignment keeps clear of the strips covering both ends of the viewport")
  func insetsBothEnds() {
    let h = Harness(rowCount: 500)
    h.settle()
    h.view.viewportInsets = (40, 30)
    for (align, top) in [(VirtualScrollAlignment.start, 40.0), (.end, 350), (.center, 195)] {
      h.core.scrollToItem("r50", align: align)
      #expect(h.rowTop("r50") == top)
      #expect(h.core.firstVisibleItem()?.index == h.visibleIndexes.first)
      #expect(h.core.lastVisibleItem()?.index == h.visibleIndexes.last)
    }
  }

  @Test("a clamped jump still releases the request, so paging keeps working")
  func clampedJump() {
    let h = Harness(rowCount: 500)
    h.settle()
    h.core.scrollToItem("r1", align: .center)
    h.settle()
    #expect(h.scrollOffset == 0)
    h.userScroll(1500)
    h.settle()
    #expect(h.items.first?.index == 56)
  }

  @Test("a loaded row keyed differently from its id is scrolled to, not re-fetched")
  func keyedDifferently() {
    let h = Harness(rowCount: 500) {
      $0.anchoring = .manual
      $0.rowKey = { "key-\($0.id)" }
    }
    h.settle()
    h.core.scrollToItem("r50", align: .start)
    var permalink = false
    for _ in 0..<4 {
      h.tick()
      permalink = permalink || h.core.queryInputs.anchor.isPermalink
    }
    #expect(h.rowTop("key-r50") == 0)
    #expect(!permalink)
  }

  @Test("addressing a rendered row by its key: scrolls, and issues no lookup at all")
  func byKey() {
    let h = Harness(rowCount: 500) {
      $0.anchoring = .manual
      $0.rowKey = { "key-\($0.id)" }
    }
    h.settle()
    h.core.scrollToItem("key-r50", align: .start)
    #expect(h.rowTop("key-r50") == 0)
    #expect(h.core.queryInputs.probeID == nil)
  }

  @Test(
    "addressing a rendered row by its key: reaches rendered rows only: an off-screen key does nothing"
  )
  func byKeyOffScreen() {
    let h = Harness(rowCount: 500) {
      $0.anchoring = .manual
      $0.rowKey = { "key-\($0.id)" }
    }
    h.settle()
    let firstBefore = h.items.first?.index
    h.core.scrollToItem("key-r400", align: .start)
    h.settle()
    #expect(h.items.first?.index == firstBefore)
    #expect(!h.core.snapshot.rowsEmpty)
    h.core.scrollToRow(key: "key-r400")
    h.settle()
    #expect(h.items.first?.index == firstBefore)
  }

  @Test("a repeat jump to a row keyed differently from its id still lands")
  func repeatJump() {
    let h = Harness(rowCount: 500) {
      $0.anchoring = .manual
      $0.rowKey = { "key-\($0.id)" }
    }
    h.settle()
    h.core.scrollToItem("r400", align: .start)
    h.settle()
    #expect(h.rowTop("key-r400") == 0)
    var notified = 0
    let unsubscribe = h.core.subscribe { notified += 1 }
    h.core.scrollToItem("r400", align: .center)
    unsubscribe()
    #expect(notified > 0)
    h.settle()
    #expect(h.rowTop("key-r400") == 190)
    h.userScroll(0)
    h.settle()
    #expect(!h.items.isEmpty)
  }

  @Test("an empty id is a no-op, not a lookup that never answers")
  func emptyID() {
    let h = Harness(rowCount: 500) { $0.anchoring = .manual }
    h.settle()
    h.core.scrollToItem("")
    h.tick()
    #expect(h.core.queryInputs.probeID == nil)
    h.core.scrollToItem("r400", align: .start)
    h.settle()
    #expect(h.rowTop("r400") == 0)
  }

  @Test("a second jump to a rendered row supersedes a lookup still in flight")
  func supersedesLookup() {
    let h = Harness(rowCount: 500) { $0.anchoring = .manual }
    h.settle()
    h.core.scrollToItem("r400", align: .start)
    h.core.scrollToItem("r50", align: .start)
    #expect(h.rowTop("r50") == 0)
    h.settle()
    #expect(h.rowTop("r50") == 0)
  }

  @Test("a second jump supersedes one that is still loading")
  func supersedesLoading() {
    let h = Harness(rowCount: 500) { $0.anchoring = .manual }
    h.settle()
    h.core.scrollToItem("r400", align: .start)
    h.core.scrollToItem("r200", align: .start)
    h.settle()
    #expect(h.rowTop("r200") == 0)
  }

  @Test("an id that does not exist: does nothing: the list and the scroll position are left alone")
  func missingIDNoop() {
    let h = Harness(rowCount: 500) { $0.anchoring = .manual }
    h.settle()
    h.userScroll(500)
    h.settle()
    let count = h.items.count
    let first = h.items.first?.index
    let top = h.rowTop("r30")
    let offset = h.scrollOffset
    h.core.scrollToItem("nope")
    h.settle()
    #expect(h.items.count == count)
    #expect(h.items.first?.index == first)
    #expect(!h.core.snapshot.rowsEmpty)
    #expect(h.rowTop("r30") == top)
    #expect(h.scrollOffset == offset)
  }

  @Test("an id that does not exist: takes its lookup back out of the query inputs")
  func missingIDLookupDropped() {
    let h = Harness(rowCount: 500) { $0.anchoring = .manual }
    h.settle()
    h.core.scrollToItem("nope")
    #expect(h.core.queryInputs.probeID == "nope")
    var notified = 0
    let unsubscribe = h.core.subscribe { notified += 1 }
    h.tick()
    unsubscribe()
    #expect(h.core.queryInputs.probeID == nil)
    #expect(notified > 0)
  }

  @Test("an id that does not exist: leaves paging working afterwards")
  func missingIDPaging() {
    let h = Harness(rowCount: 500)
    h.settle()
    h.core.scrollToItem("nope")
    h.settle()
    h.userScroll(1500)
    h.settle()
    #expect(h.items.first?.index == 56)
  }

  @Test("an id that does not exist: a later jump to a real id still lands")
  func missingIDThenReal() {
    let h = Harness(rowCount: 500) { $0.anchoring = .manual }
    h.settle()
    h.core.scrollToItem("nope")
    h.settle()
    h.core.scrollToItem("r400", align: .start)
    h.settle()
    #expect(h.rowTop("r400") == 0)
  }

  @Test("an id that does not exist: the lookup does not disturb the list while it is in flight")
  func lookupInFlight() {
    let h = Harness(rowCount: 500)
    h.settle()
    let first = h.items.first?.index
    h.core.scrollToItem("r400")
    h.tick()
    #expect(h.items.first?.index == first)
    #expect(!h.core.snapshot.rowsEmpty)
  }

  @Test("a permalink that does not exist leaves the list alone even mid-lookup")
  func missingPermalinkMidLookup() {
    let h = Harness(rowCount: 500) { $0.anchoring = .manual }
    h.settle()
    h.userScroll(500)
    h.settle()
    let first = h.items.first?.index
    h.core.scrollToItem("r400")
    h.setOptions { $0.permalinkID = "nope" }
    h.settle()
    #expect(!h.core.snapshot.rowsEmpty)
    #expect(h.items.first?.index == first)
  }

  @Test("a permalinkID that does not exist leaves a loaded list alone")
  func missingPermalinkLoaded() {
    let h = Harness(rowCount: 500) { $0.anchoring = .manual }
    h.settle()
    let count = h.items.count
    h.setOptions { $0.permalinkID = "nope" }
    h.settle()
    #expect(h.items.count == count)
    #expect(!h.core.snapshot.rowsEmpty)
  }

  @Test("a deep link to a permalinkID that does not exist falls back to the top of the list")
  func missingPermalinkColdLoad() {
    let h = Harness(rowCount: 500) { $0.permalinkID = "nope" }
    h.tick()
    h.tick()
    h.settle()
    #expect(!h.core.snapshot.rowsEmpty)
    #expect(h.items.first?.index == 0)
    #expect(h.items.first?.row == TestRow(id: "r0"))
  }
}

/// A minimal host for a core whose start rows aren't its rows.
@MainActor
private final class CursorScrollView: VirtualHost {
  typealias Core = VirtualizerCore<String, TestRow, String, StartRowTests.Cursor>
  let core: Core
  let data: [TestRow]
  var items: [VirtualItem<TestRow, String>] = []
  var spaceBefore = 0.0
  var offset = 0.0
  let viewportHeight = 400.0
  let viewportInsets: (top: Double, bottom: Double) = (0, 0)

  init(core: Core, data: [TestRow]) {
    self.core = core
    self.data = data
  }

  var scrollOffset: Double { offset }
  var contentHeight: Double { spaceBefore + Double(items.count) * 20 + core.snapshot.spaceAfter }
  func scroll(to target: Double) { offset = max(0, min(target, contentHeight - viewportHeight)) }
  func rowFrames() -> [VirtualRowFrame] {
    items.enumerated().map { i, item in
      let y = spaceBefore + Double(i) * 20
      return VirtualRowFrame(index: item.index, key: AnyHashable(item.key), minY: y, maxY: y + 20)
    }
  }

  func scroll(by delta: Double) {
    scroll(to: offset + delta)
    core.didScroll()
  }

  func settle() {
    for _ in 0..<20 {
      let inputs = core.queryInputs
      let (start, direction): (StartRowTests.Cursor?, VirtualDirection) =
        switch inputs.anchor {
        case .forward(_, let s): (s, .forward)
        case .backward(_, let s): (s, .backward)
        case .permalink: (nil, .forward)
        }
      let i =
        start.flatMap { s in data.firstIndex { $0.id == s.id } }
        ?? (direction == .forward ? -1 : data.count)
      let page =
        direction == .forward
        ? Array(data[min(data.count, i + 1)..<min(data.count, i + 1 + inputs.pageSize + 1)])
        : Array(data[max(0, i - inputs.pageSize - 1)..<i].reversed())
      core.setRows(assembleRows(inputs, RowsQueryResults(mainRows: page, mainComplete: true)))
      items = core.snapshot.items
      spaceBefore = core.snapshot.spaceBefore
      core.attach(self)
      core.afterUpdate()
      if core.queryInputs == inputs { return }
    }
  }
}
