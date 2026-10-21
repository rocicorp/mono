import Testing

@testable import ZeroVirtual

/// zero-virtual's stick-to-bottom: `demo/react/e2e/tests/stick.spec.ts` and
/// the `useStickToBottom` case of `use-zero-virtualizer.test.ts`.
@MainActor
@Suite("stick to bottom") struct StickToBottomTests {
  func chat(rows: Int) -> Harness {
    let h = Harness(rowCount: rows) {
      $0.stickToBottom = true
      $0.anchoring = .manual
    }
    h.settle()
    return h
  }

  func append(_ h: Harness, _ n: Int) {
    let start = h.data.count
    h.data += (start..<start + n).map { TestRow(id: "r\($0)") }
  }

  var bottomGap: (Harness) -> Double {
    { $0.view.contentHeight - $0.scrollOffset - $0.view.viewportHeight }
  }

  @Test("a list that starts short follows rows added below it once they overflow")
  func startsStuck() {
    let h = chat(rows: 10)
    append(h, 30)
    h.settle()
    #expect(bottomGap(h) == 0)
    #expect(h.visibleIndexes.last == 39)
  }

  @Test("parked at the bottom, it follows new rows and a last row growing taller")
  func follows() {
    let h = chat(rows: 60)
    h.userScroll(.infinity)
    h.settle()
    #expect(bottomGap(h) == 0)
    append(h, 3)
    h.settle()
    #expect(h.visibleIndexes.last == 62)
    #expect(bottomGap(h) == 0)
    h.setRowHeight("r62", 120)
    h.tick()
    #expect(bottomGap(h) == 0)
  }

  @Test("scrolled away, growth does not follow; back at the bottom, it re-arms")
  func rearms() {
    let h = chat(rows: 60)
    h.userScroll(.infinity)
    h.settle()
    h.userScroll(h.scrollOffset - 300)
    let offset = h.scrollOffset
    append(h, 3)
    h.settle()
    #expect(h.scrollOffset == offset)
    #expect(bottomGap(h) > 300)

    h.userScroll(.infinity)
    h.settle()
    append(h, 3)
    h.settle()
    #expect(h.visibleIndexes.last == 65)
    #expect(bottomGap(h) == 0)
  }

  @Test("a list not asked to stick stays where it was")
  func off() {
    let h = Harness(rowCount: 60) { $0.anchoring = .manual }
    h.settle()
    h.userScroll(.infinity)
    h.settle()
    let offset = h.scrollOffset
    append(h, 3)
    h.settle()
    #expect(h.scrollOffset == offset)
  }

  @Test("the viewport shrinking keeps the bottom in view")
  func resize() {
    let h = chat(rows: 60)
    h.userScroll(.infinity)
    h.settle()
    h.view.viewportHeight = 200
    h.core.viewportDidResize()
    #expect(bottomGap(h) == 0)
  }
}

extension StickToBottomTests {
  @Test("scrolled up, scrolling to the last row with .end follows again")
  func rearmsAfterScrollToItem() {
    let h = chat(rows: 200)
    h.core.scrollToItem("r199", align: .end)
    h.settle()
    #expect(bottomGap(h) == 0)
    h.userScroll(h.scrollOffset - 300)
    h.settle()
    append(h, 1)
    h.settle()
    #expect(bottomGap(h) > 300)
    h.core.scrollToItem("r200", align: .end)
    h.settle()
    #expect(bottomGap(h) == 0)
    append(h, 1)
    h.settle()
    #expect(h.core.lastVisibleItem()?.key == "r201")
    #expect(bottomGap(h) == 0)
  }
}
