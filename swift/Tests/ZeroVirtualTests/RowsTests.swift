import Testing

@testable import ZeroVirtual

/// zero-virtual's `src/core/rows.test.ts`.
@Suite("row window boundaries") struct RowsTests {
  let complete = RowsQueryResults<String>(
    singleComplete: true, mainComplete: true, afterComplete: true)

  @Test(
    "%s pages exclude the sentinel and retain query order",
    arguments: [VirtualDirection.forward, .backward])
  func pagesExcludeTheSentinel(direction: VirtualDirection) {
    var results = complete
    results.mainRows = ["a", "b", "c", "d", "sentinel"]
    let anchor: VirtualAnchor<String> =
      direction == .forward
      ? .forward(index: 10, startRow: "cursor") : .backward(index: 10, startRow: "cursor")
    let rows = assembleRows(
      RowsQueryInputs(pageSize: 4, anchor: anchor, settled: false, probeID: nil), results)
    let first = direction == .forward ? 10 : 6
    #expect(rows.firstRowIndex == first)
    #expect(rows.rowsLength == 4)
    #expect(!rows.atStart)
    #expect(!rows.atEnd)
    #expect(
      (0..<4).map { rows.rowAt(first + $0) }
        == (direction == .forward ? ["a", "b", "c", "d"] : ["d", "c", "b", "a"]))
    #expect(rows.rowAt(first - 1) == nil)
    #expect(rows.rowAt(first + 4) == nil)
  }

  @Test("permalink pages reserve the center row and exclude both sentinels")
  func permalinkPages() {
    var results = complete
    results.singleRow = "center"
    results.mainRows = ["before", "first", "sentinel"]
    results.afterRows = ["after", "sentinel"]
    let rows = assembleRows(
      RowsQueryInputs<String>(
        pageSize: 4, anchor: .permalink(index: 1, id: "center"), settled: false, probeID: nil),
      results)
    #expect(rows.firstRowIndex == -1)
    #expect(rows.rowsLength == 4)
    #expect(
      [-2, -1, 0, 1, 2, 3].map { rows.rowAt($0) } == [
        nil, "first", "before", "center", "after", nil,
      ]
    )
    #expect(!rows.atStart)
    #expect(!rows.atEnd)
  }

  @Test("a lone permalink retains the existing empty-window semantics")
  func lonePermalink() {
    var results = complete
    results.singleRow = "only"
    results.mainRows = []
    results.afterRows = []
    let rows = assembleRows(
      RowsQueryInputs<String>(
        pageSize: 4, anchor: .permalink(index: 1, id: "only"), settled: false, probeID: nil),
      results)
    #expect(rows.rowsLength == 1)
    #expect(rows.rowsEmpty)
    #expect(rows.rowAt(1) == "only")
    #expect(rows.atStart && rows.atEnd && rows.complete)
  }

  // From `create-rows.test.ts`, which checks the queries the window runs.
  @Test("a page anchor asks for one row more than the page")
  func pageQuery() {
    let inputs = RowsQueryInputs<String>(
      pageSize: 50, anchor: .forward(index: 0, startRow: nil), settled: false, probeID: nil)
    let (main, after) = pageRequests(inputs, toStartRow: { (r: String) in r }, singleRow: nil)
    #expect(main == VirtualPageRequest(limit: 51, start: nil, direction: .forward, settled: false))
    #expect(after == nil)
    #expect(singleRequest(inputs) == nil)
  }

  @Test("a permalink looks its row up, then pages either side of it")
  func permalinkQueries() {
    let inputs = RowsQueryInputs<String>(
      pageSize: 50, anchor: .permalink(index: 1, id: "x"), settled: true, probeID: nil)
    #expect(singleRequest(inputs) == VirtualRowRequest(id: "x", settled: true))
    let waiting = pageRequests(inputs, toStartRow: { (r: String) in r }, singleRow: nil)
    #expect(waiting.main == nil && waiting.after == nil)
    let (main, after) = pageRequests(
      inputs, toStartRow: { (r: String) in "start:" + r }, singleRow: "x")
    #expect(
      main == VirtualPageRequest(limit: 26, start: "start:x", direction: .backward, settled: true))
    #expect(
      after == VirtualPageRequest(limit: 25, start: "start:x", direction: .forward, settled: true))
  }
}
