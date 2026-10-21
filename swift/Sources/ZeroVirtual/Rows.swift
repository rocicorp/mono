/// Which way a page query reads from its start row.
public enum VirtualDirection: String, Sendable, Codable, Hashable {
  case forward, backward
}

/// Where the loaded window is anchored in the list (`Anchor` in
/// zero-virtual's `types.ts`).
///
/// - `forward`: the page after `startRow` (the list's start when nil), its
///   first row at `index`.
/// - `backward`: the page before `startRow`, its last row at `index - 1`.
/// - `permalink`: the row with `id` at `index`, half a page either side.
public enum VirtualAnchor<StartRow: Equatable & Sendable>: Equatable, Sendable {
  case forward(index: Int, startRow: StartRow?)
  case backward(index: Int, startRow: StartRow)
  case permalink(index: Int, id: String)

  public var index: Int {
    switch self {
    case .forward(let index, _), .backward(let index, _), .permalink(let index, _): index
    }
  }

  func with(index: Int) -> Self {
    switch self {
    case .forward(_, let startRow): .forward(index: index, startRow: startRow)
    case .backward(_, let startRow): .backward(index: index, startRow: startRow)
    case .permalink(_, let id): .permalink(index: index, id: id)
    }
  }

  var isPermalink: Bool {
    if case .permalink = self { return true }
    return false
  }

  static var top: Self { .forward(index: 0, startRow: nil) }
}

extension VirtualAnchor: Codable where StartRow: Codable {
  private enum CodingKeys: String, CodingKey { case kind, index, startRow, id }

  public init(from decoder: any Decoder) throws {
    let c = try decoder.container(keyedBy: CodingKeys.self)
    let index = try c.decode(Int.self, forKey: .index)
    switch try c.decode(String.self, forKey: .kind) {
    case "forward":
      self = .forward(
        index: index, startRow: try c.decodeIfPresent(StartRow.self, forKey: .startRow))
    case "backward":
      self = .backward(index: index, startRow: try c.decode(StartRow.self, forKey: .startRow))
    case "permalink":
      self = .permalink(index: index, id: try c.decode(String.self, forKey: .id))
    case let kind:
      throw DecodingError.dataCorruptedError(
        forKey: .kind, in: c, debugDescription: "Unknown anchor kind \(kind)")
    }
  }

  public func encode(to encoder: any Encoder) throws {
    var c = encoder.container(keyedBy: CodingKeys.self)
    try c.encode(index, forKey: .index)
    switch self {
    case .forward(_, let startRow):
      try c.encode("forward", forKey: .kind)
      try c.encodeIfPresent(startRow, forKey: .startRow)
    case .backward(_, let startRow):
      try c.encode("backward", forKey: .kind)
      try c.encode(startRow, forKey: .startRow)
    case .permalink(_, let id):
      try c.encode("permalink", forKey: .kind)
      try c.encode(id, forKey: .id)
    }
  }
}

/// What a page query is asked for (`GetPageQueryOptions`).
public struct VirtualPageRequest<StartRow: Equatable & Sendable>: Equatable, Sendable {
  /// How many rows to read, one more than the page to tell whether more
  /// follow.
  public var limit: Int
  /// The row to read from, exclusive; nil for the list's start (forward)
  /// or end (backward).
  public var start: StartRow?
  /// Backward pages read the rows before `start`, closest first.
  public var direction: VirtualDirection
  /// Whether the list has been idle for the settle time.
  public var settled: Bool
}

/// What the single-row query is asked for (`GetSingleQueryOptions`).
public struct VirtualRowRequest: Equatable, Sendable {
  /// The id the app addresses the row by: a permalink or `scrollToItem`'s.
  public var id: String
  /// Whether the list has been idle for the settle time.
  public var settled: Bool
}

/// The inputs that decide which queries run (`RowsQueryInputs`).
package struct RowsQueryInputs<StartRow: Equatable & Sendable>: Equatable, Sendable {
  package var pageSize: Int
  package var anchor: VirtualAnchor<StartRow>
  package var settled: Bool
  /// An id to look up without moving the window: `scrollToItem` checks a
  /// row exists before re-anchoring on it. Nil under a permalink anchor,
  /// which runs its own lookup.
  package var probeID: String?
}

/// The raw results of the (up to) three queries (`RowsQueryResults`).
package struct RowsQueryResults<Row> {
  package var singleRow: Row?
  package var singleComplete = false
  /// The page before a permalink's row, or the whole page.
  package var mainRows: [Row]?
  package var mainComplete = false
  /// The page after a permalink's row.
  package var afterRows: [Row]?
  package var afterComplete = false

  package init(
    singleRow: Row? = nil, singleComplete: Bool = false, mainRows: [Row]? = nil,
    mainComplete: Bool = false, afterRows: [Row]? = nil, afterComplete: Bool = false
  ) {
    self.singleRow = singleRow
    self.singleComplete = singleComplete
    self.mainRows = mainRows
    self.mainComplete = mainComplete
    self.afterRows = afterRows
    self.afterComplete = afterComplete
  }
}

/// The loaded rows as a window of the list (`RowsSnapshot`).
package struct RowsSnapshot<Row, StartRow: Equatable & Sendable> {
  /// The anchor these rows answer, whose frame `firstRowIndex` counts in.
  /// It lags a re-anchor until the new window's rows arrive.
  package var anchor: VirtualAnchor<StartRow>
  /// The page size these rows answer.
  package var pageSize: Int
  package var rowAt: (Int) -> Row?
  package var rowsLength: Int
  package var complete: Bool
  package var rowsEmpty: Bool
  package var atStart: Bool
  package var atEnd: Bool
  package var firstRowIndex: Int
  /// A permalink anchor's row doesn't exist.
  package var permalinkNotFound: Bool
  /// The single-row lookup: its id, the row it found, and whether it
  /// completed. Check the id first: it may answer a previous lookup.
  package var lookup: (id: String, row: Row?, complete: Bool)?

  package static var empty: Self {
    RowsSnapshot(
      anchor: .top, pageSize: 0, rowAt: { _ in nil }, rowsLength: 0, complete: false,
      rowsEmpty: true, atStart: false, atEnd: false, firstRowIndex: 0, permalinkNotFound: false,
      lookup: nil)
  }
}

/// The single-row lookup: a permalink anchor's row, or the probe's.
package func singleRequest<StartRow>(_ inputs: RowsQueryInputs<StartRow>) -> VirtualRowRequest? {
  if case .permalink(_, let id) = inputs.anchor {
    precondition(inputs.probeID == nil, "a probe and a permalink share the lookup")
    return VirtualRowRequest(id: id, settled: inputs.settled)
  }
  return inputs.probeID.map { VirtualRowRequest(id: $0, settled: inputs.settled) }
}

/// The page queries: under a page anchor the whole page, one row more than
/// the page; under a permalink anchor the pages before and after its row,
/// once the lookup has found it.
package func pageRequests<Row, StartRow>(
  _ inputs: RowsQueryInputs<StartRow>, toStartRow: (Row) -> StartRow, singleRow: Row?
) -> (main: VirtualPageRequest<StartRow>?, after: VirtualPageRequest<StartRow>?) {
  let settled = inputs.settled
  switch inputs.anchor {
  case .forward(_, let start):
    return (
      VirtualPageRequest(
        limit: inputs.pageSize + 1, start: start, direction: .forward, settled: settled), nil
    )
  case .backward(_, let start):
    return (
      VirtualPageRequest(
        limit: inputs.pageSize + 1, start: start, direction: .backward, settled: settled), nil
    )
  case .permalink:
    precondition(inputs.pageSize % 2 == 0)
    guard let singleRow else { return (nil, nil) }
    let start = toStartRow(singleRow)
    let half = inputs.pageSize / 2
    return (
      VirtualPageRequest(limit: half + 1, start: start, direction: .backward, settled: settled),
      VirtualPageRequest(limit: half, start: start, direction: .forward, settled: settled)
    )
  }
}

/// The window the query results make (`assembleRows`).
package func assembleRows<Row, StartRow>(
  _ inputs: RowsQueryInputs<StartRow>, _ results: RowsQueryResults<Row>
) -> RowsSnapshot<Row, StartRow> {
  let anchor = inputs.anchor
  let anchorIndex = anchor.index
  let half = inputs.pageSize / 2
  let r = results

  let lookupID: String?
  var permalinkNotFound = false
  if case .permalink(_, let id) = anchor {
    lookupID = id
    permalinkNotFound = r.singleComplete && r.singleRow == nil
  } else {
    lookupID = inputs.probeID
  }
  let lookup = lookupID.map { (id: $0, row: r.singleRow, complete: r.singleComplete) }

  let beforeLength = r.mainRows?.count ?? 0
  let afterLength = r.afterRows?.count ?? 0

  switch anchor {
  case .permalink:
    let beforeSize = min(beforeLength, half)
    let afterSize = min(afterLength, half - 1)
    let single = r.singleRow
    let main = r.mainRows ?? []
    let after = r.afterRows ?? []
    let rowAt: (Int) -> Row? = { index in
      if index == anchorIndex { return single }
      if index > anchorIndex {
        let offset = index - anchorIndex - 1
        return offset < afterSize ? after[offset] : nil
      }
      let offset = anchorIndex - index - 1
      return offset < beforeSize ? main[offset] : nil
    }
    return RowsSnapshot(
      anchor: anchor, pageSize: inputs.pageSize, rowAt: rowAt,
      rowsLength: permalinkNotFound ? 0 : beforeSize + afterSize + (single == nil ? 0 : 1),
      complete: r.singleComplete && (permalinkNotFound || (r.mainComplete && r.afterComplete)),
      rowsEmpty: permalinkNotFound || single == nil || (beforeSize == 0 && afterSize == 0),
      atStart: permalinkNotFound || (r.mainComplete && beforeLength <= half),
      atEnd: permalinkNotFound || (r.afterComplete && afterLength <= half - 1),
      firstRowIndex: permalinkNotFound ? anchorIndex : anchorIndex - beforeSize,
      permalinkNotFound: permalinkNotFound, lookup: lookup)

  case .forward, .backward:
    let forward: Bool
    let fromStart: Bool
    if case .forward(_, let start) = anchor {
      forward = true
      fromStart = start == nil
    } else {
      forward = false
      fromStart = false
    }
    let page = r.mainRows ?? []
    let hasMore = page.count > inputs.pageSize
    let length = hasMore ? inputs.pageSize : page.count
    let rowAt: (Int) -> Row? = { index in
      let offset = forward ? index - anchorIndex : anchorIndex - index - 1
      return offset >= 0 && offset < length ? page[offset] : nil
    }
    return RowsSnapshot(
      anchor: anchor, pageSize: inputs.pageSize, rowAt: rowAt, rowsLength: length,
      complete: r.mainComplete, rowsEmpty: page.isEmpty,
      atStart: forward ? fromStart || anchorIndex == 0 : r.mainComplete && !hasMore,
      atEnd: forward && r.mainComplete && !hasMore,
      firstRowIndex: forward ? anchorIndex : anchorIndex - length,
      permalinkNotFound: permalinkNotFound, lookup: lookup)
  }
}
