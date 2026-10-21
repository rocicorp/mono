import Testing
import ZeroTesting

@testable import ZeroVirtual

/// zero-virtual's `src/core/virtualizer.test.ts`: the core without a scroll
/// view.
@MainActor
@Suite struct VirtualizerTests {
  typealias Core = VirtualizerCore<String, TestRow, String, TestRow>
  nonisolated static let est = 48.0

  static func options(_ change: (inout Core.Options) -> Void = { _ in }) -> Core.Options {
    var o = Core.Options(
      estimatedRowHeight: est, rowKey: \.id, toStartRow: { $0 }, listContext: "ctx")
    change(&o)
    return o
  }

  /// Rows with nothing loaded, changed by `change`.
  static func rows(
    _ change: (inout RowsSnapshot<TestRow, TestRow>) -> Void = { _ in }
  ) -> RowsSnapshot<TestRow, TestRow> {
    var r = RowsSnapshot<TestRow, TestRow>.empty
    r.pageSize = 50
    change(&r)
    return r
  }

  static func loaded(
    length: Int, first: Int, complete: Bool, atStart: Bool, atEnd: Bool
  ) -> RowsSnapshot<TestRow, TestRow> {
    rows {
      $0.rowsLength = length
      $0.firstRowIndex = first
      $0.complete = complete
      $0.rowsEmpty = length == 0
      $0.atStart = atStart
      $0.atEnd = atEnd
      $0.rowAt = { TestRow(id: "r\($0)") }
    }
  }

  static func core(
    _ rows: RowsSnapshot<TestRow, TestRow>, _ change: (inout Core.Options) -> Void = { _ in }
  ) -> Core {
    let core = Core(options(change), scheduler: ManualScheduler())
    core.setRows(rows)
    return core
  }

  struct SnapshotCase: CustomTestStringConvertible, Sendable {
    let name: String
    let length, first: Int
    let complete, atStart, atEnd: Bool
    let items: Int
    let spaceBefore: Double?
    let spaceAfter: Double?
    let total: Int?
    var testDescription: String { name }
  }

  @Test(
    "ZeroVirtualizer snapshot — items, space and total",
    arguments: [
      SnapshotCase(
        name: "empty, loading (no rows yet)", length: 0, first: 0, complete: false, atStart: false,
        atEnd: false, items: 0, spaceBefore: 0, spaceAfter: nil, total: nil),
      SnapshotCase(
        name: "empty, complete", length: 0, first: 0, complete: true, atStart: true, atEnd: true,
        items: 0, spaceBefore: 0, spaceAfter: 0, total: 0),
      SnapshotCase(
        name: "loading from top, some rows", length: 20, first: 0, complete: false, atStart: true,
        atEnd: false, items: 20, spaceBefore: 0, spaceAfter: nil, total: nil),
      SnapshotCase(
        name: "all rows loaded", length: 20, first: 0, complete: true, atStart: true, atEnd: true,
        items: 20, spaceBefore: 0, spaceAfter: 0, total: 20),
      SnapshotCase(
        name: "loading at end, rows above (firstRowIndex>0)", length: 20, first: 5,
        complete: false, atStart: false, atEnd: true, items: 20, spaceBefore: 5 * est,
        spaceAfter: 0, total: nil),
      SnapshotCase(
        name: "complete in middle (firstRowIndex>0)", length: 50, first: 10, complete: true,
        atStart: false, atEnd: false, items: 50, spaceBefore: 10 * est, spaceAfter: nil, total: nil),
    ])
  func snapshot(c: SnapshotCase) {
    let s = Self.core(
      Self.loaded(
        length: c.length, first: c.first, complete: c.complete, atStart: c.atStart, atEnd: c.atEnd)
    ).snapshot
    #expect(s.items.count == c.items)
    #expect(s.total == c.total)
    if let b = c.spaceBefore { #expect(s.spaceBefore == b) }
    if let a = c.spaceAfter { #expect(s.spaceAfter == a) }
  }

  @Test("items carry the correct index and row")
  func itemsCarryIndexAndRow() {
    let data = ["a", "b", "c"].map { TestRow(id: $0) }
    let s = Self.core(
      Self.rows {
        $0.rowsLength = 3
        $0.complete = true
        $0.rowsEmpty = false
        $0.firstRowIndex = 7
        $0.rowAt = { i in (7..<10).contains(i) ? data[i - 7] : nil }
      }
    ).snapshot
    #expect(s.items.map(\.index) == [7, 8, 9])
    #expect(s.items.map(\.key) == ["a", "b", "c"])
    #expect(s.items[0].row == TestRow(id: "a"))
  }

  @Test("changing estimateSize invalidates the cached snapshot")
  func changingEstimate() {
    let core = Self.core(
      Self.loaded(length: 20, first: 5, complete: true, atStart: false, atEnd: false))
    #expect(core.snapshot.spaceBefore == 5 * Self.est)
    core.setOptions(Self.options { $0.estimatedRowHeight = 10 })
    #expect(core.snapshot.spaceBefore == 5 * 10)
  }

  @Test("scrolling notifies the settled → false transition")
  func scrollingUnsettles() {
    let scheduler = ManualScheduler()
    let core = Core(Self.options(), scheduler: scheduler)
    let view = FakeScrollView(viewportHeight: 400, rowHeight: 48)
    view.core = core
    core.attach(view)

    scheduler.advance(by: 2000)
    #expect(core.snapshot.settled)

    var notified = 0
    _ = core.subscribe { notified += 1 }
    core.didScroll()
    #expect(notified > 0)
    #expect(!core.snapshot.settled)
    core.detach()
  }

  @Test("setRows/setOptions are silent; afterDOMUpdate notifies on transitions")
  func silentIngestion() {
    let core = Core(Self.options(), scheduler: ManualScheduler())
    var notified = 0
    _ = core.subscribe { notified += 1 }
    core.setRows(Self.loaded(length: 10, first: 0, complete: true, atStart: true, atEnd: false))
    core.setOptions(Self.options())
    #expect(notified == 0)
    core.afterUpdate()
    #expect(notified > 0)
    #expect(core.queryInputs.anchor == .forward(index: 0, startRow: nil))
  }

  @Test("query inputs fall back to the new context after a context change")
  func contextFallback() {
    let core = Core(Self.options { $0.listContext = "a" }, scheduler: ManualScheduler())
    #expect(core.queryInputs.anchor == .forward(index: 0, startRow: nil))
    core.setOptions(Self.options { $0.listContext = "b" })
    #expect(core.queryInputs.anchor == .forward(index: 0, startRow: nil))
    core.setOptions(
      Self.options {
        $0.listContext = "c"
        $0.permalinkID = "row-9"
      })
    #expect(core.queryInputs.anchor == .permalink(index: 1, id: "row-9"))
  }

  @Test("minPageSize defaults the query page size to 50")
  func minPageSizeDefault() {
    #expect(Core(Self.options(), scheduler: ManualScheduler()).queryInputs.pageSize == 50)
  }

  @Test("minPageSize lowers the floor for tall rows")
  func minPageSizeLowers() {
    let core = Core(Self.options { $0.minPageSize = 20 }, scheduler: ManualScheduler())
    #expect(core.queryInputs.pageSize == 20)
  }

  @Test("minPageSize is rounded up to an even number (paging halves pages)")
  func minPageSizeEven() {
    let core = Core(Self.options { $0.minPageSize = 15 }, scheduler: ManualScheduler())
    #expect(core.queryInputs.pageSize == 16)
  }
}
