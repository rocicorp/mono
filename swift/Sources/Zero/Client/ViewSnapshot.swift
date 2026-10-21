/// The identity of a row in a view: its primary key values, in key order.
public struct RowID: Hashable, Sendable, CustomStringConvertible {
  public let values: [JSONValue]

  public init(_ values: [JSONValue]) { self.values = values }

  init(_ row: Row, primaryKey: [String]) {
    values = primaryKey.map { row[$0] ?? .null }
  }

  /// A single-column key as JS `String()` prints it, a compound key as a
  /// JSON array.
  public var description: String {
    values.count == 1 ? Keys.jsString(values[0]) : JSONSerializer.stringify(.array(values))
  }
}

/// A view's rows at one point, and how its top-level rows changed since the
/// snapshot before.
public struct ViewSnapshot: Sendable {
  public let data: ViewValue
  public let resultType: ResultType
  public let error: ErroredQuery?
  /// The IDs of the top-level rows, in order.
  public let ids: [RowID]
  /// From the previous snapshot's `ids` to these. The first snapshot inserts
  /// every row.
  public let difference: CollectionDifference<RowID>
  /// The rows in both snapshots whose columns or related rows changed.
  public let updated: Set<RowID>
}

/// Diffs each value of a view against the one before.
struct RowDiffer {
  let primaryKey: [String]
  private var ids: [RowID] = []
  private var entries: [RowID: ViewEntry] = [:]

  init(primaryKey: [String]) { self.primaryKey = primaryKey }

  mutating func next(
    _ data: ViewValue, _ resultType: ResultType, _ error: ErroredQuery?
  ) -> ViewSnapshot {
    let rows = data.entries
    var newIDs: [RowID] = []
    newIDs.reserveCapacity(rows.count)
    var newEntries: [RowID: ViewEntry] = [:]
    newEntries.reserveCapacity(rows.count)
    var updated: Set<RowID> = []
    for entry in rows {
      let id = RowID(entry.row, primaryKey: primaryKey)
      newIDs.append(id)
      if let old = entries[id], old != entry { updated.insert(id) }
      newEntries[id] = entry
    }
    // Most changes edit rows in place, which leaves the order as it was.
    let difference =
      newIDs == ids ? CollectionDifference<RowID>([])! : newIDs.difference(from: ids)
    ids = newIDs
    entries = newEntries
    return ViewSnapshot(
      data: data, resultType: resultType, error: error, ids: newIDs, difference: difference,
      updated: updated)
  }
}
