/// A source of rows held in memory, kept in sorted order
/// (`packages/zql/src/ivm/memory-source.ts`).
package final class MemorySource: Source {
  struct Overlay {
    var epoch: Int
    var change: SourceChange
  }

  struct Overlays {
    var add: Row?
    var remove: Row?
  }

  struct Index {
    var comparator: BoundComparator
    var data: BTreeSet<Row>
  }

  final class Connection {
    var input: MemorySourceInput!
    weak var output: (any Output)?
    let sort: AST.Ordering
    let splitEditKeys: Set<String>?
    let compareRows: Comparator
    let filters: (condition: AST.Condition, predicate: RowPredicate)?
    var lastPushedEpoch = 0

    init(
      sort: AST.Ordering, splitEditKeys: Set<String>?, compareRows: @escaping Comparator,
      filters: (condition: AST.Condition, predicate: RowPredicate)?
    ) {
      self.sort = sort
      self.splitEditKeys = splitEditKeys
      self.compareRows = compareRows
      self.filters = filters
    }
  }

  let tableName: String
  let columns: [String: ColumnSchema]
  let primaryKey: [String]
  let primaryIndexSort: AST.Ordering
  let primaryIndexKey: String
  var indexes: [String: Index] = [:]
  var indexOrder: [String] = []
  var connections: [Connection] = []
  var overlay: Overlay?
  var pushEpoch = 0
  private let schema: TableSchema
  /// The layout of the source's rows: its table's columns.
  package let layout: RowLayout

  package convenience init(_ table: TableSchema) {
    self.init(table, layout: RowLayout(table))
  }

  /// A source whose rows are in `layout`, which has `table`'s columns.
  package convenience init(_ table: TableSchema, layout: RowLayout) {
    self.init(table: table, layout: layout, primaryIndexData: nil)
  }

  init(table: TableSchema, layout: RowLayout, primaryIndexData: BTreeSet<Row>?) {
    schema = table
    self.layout = layout
    tableName = table.name
    columns = table.columns
    primaryKey = table.primaryKey
    primaryIndexSort = table.primaryKey.map { AST.OrderPart($0, .asc) }
    let comparator = BoundComparator(primaryIndexSort, layout: layout)
    primaryIndexKey = Self.indexKey(primaryIndexSort)
    indexes[primaryIndexKey] = Index(
      comparator: comparator, data: primaryIndexData ?? BTreeSet(comparator.compareRows))
    indexOrder.append(primaryIndexKey)
  }

  package var tableSchema: TableSchema { schema }

  /// A copy whose primary index shares nodes with this one until either
  /// changes.
  package func fork() -> MemorySource {
    MemorySource(table: schema, layout: layout, primaryIndexData: primaryIndex.data)
  }

  /// The rows, in primary key order.
  package var data: BTreeSet<Row> { primaryIndex.data }

  static func indexKey(_ sort: AST.Ordering) -> String {
    JSONSerializer.stringify(
      .array(sort.map { [.string($0.field), .string($0.direction.rawValue)] }))
  }

  var primaryIndex: Index { indexes[primaryIndexKey]! }

  package func connect(
    sort: AST.Ordering?, filters: AST.Condition?, splitEditKeys: Set<String>?
  ) throws(PipelineError) -> any SourceInput {
    let transformed = transformFilters(filters)
    let unordered = sort == nil
    let internalSort = sort ?? primaryIndexSort
    var connFilters: (condition: AST.Condition, predicate: RowPredicate)?
    if let condition = transformed.filters {
      connFilters = (condition, try createPredicate(condition))
    }
    let connection = Connection(
      sort: internalSort, splitEditKeys: splitEditKeys,
      compareRows: makeComparator(internalSort, layout: layout), filters: connFilters)
    let schema = SourceSchema(
      tableName: tableName, columns: columns, primaryKey: primaryKey,
      compareRows: connection.compareRows, sort: unordered ? nil : internalSort)
    if !unordered {
      try assertOrderingIncludesPK(internalSort, primaryKey)
    }
    let input = MemorySourceInput(
      source: self, connection: connection, schema: schema,
      fullyAppliedFilters: !transformed.conditionsRemoved)
    connection.input = input
    connections.append(connection)
    return input
  }

  func disconnect(_ input: MemorySourceInput) {
    guard let i = connections.firstIndex(where: { $0.input === input }) else {
      preconditionFailure("Connection not found")
    }
    // Indexes are kept for the life of the source (see the JS comment).
    connections[i].input = nil
    connections.remove(at: i)
  }

  func getOrCreateIndex(_ sort: AST.Ordering) -> Index {
    let key = Self.indexKey(sort)
    if let index = indexes[key] { return index }
    let comparator = BoundComparator(sort, layout: layout)
    let rows = Array(primaryIndex.data.values()).sorted { comparator.compareRows($0, $1) < 0 }
    let index = Index(comparator: comparator, data: BTreeSet(sorted: rows, comparator.compareRows))
    indexes[key] = index
    indexOrder.append(key)
    return index
  }

  /// For tests: the keys of the indexes built so far.
  package var indexKeys: [String] { indexOrder }

  func fetch(_ req: FetchRequest, _ conn: Connection) -> NodeStream {
    NodeStream { [self] in self.fetchIterator(req, conn) }
  }

  private func fetchIterator(_ req: FetchRequest, _ conn: Connection) -> AnyIterator<Node> {
    if let multis = req.multiConstraints, multis.contains(where: { !$0.isEmpty }) {
      return AnyIterator(fetchMulti(req, conn).makeIterator())
    }
    let requestedSort = conn.sort
    let compareRows = conn.compareRows
    let connectionComparator: Comparator = req.reverse ? { compareRows($1, $0) } : compareRows

    let reqFilter = req.filter
    let connFilterCondition = conn.filters?.condition
    let mergedFilterCondition: AST.Condition? =
      if let connFilterCondition, let reqFilter {
        .and([connFilterCondition, reqFilter])
      } else {
        connFilterCondition ?? reqFilter
      }
    let mergedFilterPredicate = mergePredicates(conn.filters?.predicate, reqFilter)

    let pkConstraint = primaryKeyConstraintFromFilters(mergedFilterCondition, primaryKey)
    // The primary key constraint is more limiting than the constraint.
    let fetchOrPkConstraint = pkConstraint ?? req.constraint

    var indexSort: AST.Ordering = []
    if let fetchOrPkConstraint {
      for key in fetchOrPkConstraint.keys { indexSort.append(AST.OrderPart(key, .asc)) }
    }
    // Constraining by the whole primary key yields one row, so the requested
    // sort does not matter.
    if primaryKey.count > 1 || fetchOrPkConstraint == nil
      || !fetchOrPkConstraint!.matchesPrimaryKey(primaryKey)
    {
      indexSort.append(contentsOf: requestedSort)
    }

    let index = getOrCreateIndex(indexSort)
    let data = index.data
    let compare = index.comparator.compareRows
    let indexComparator: Comparator = req.reverse ? { compare($1, $0) } : compare

    let startAt = req.start?.row

    // Start a constrained scan at the first row with the constraint's
    // values, whatever the direction of the following sort parts.
    let probe: ((Row) -> Int)?
    if let fetchOrPkConstraint {
      var scanStart: [String: Bound] = [:]
      for part in indexSort {
        if fetchOrPkConstraint.has(part.field) {
          scanStart[part.field] = .value(fetchOrPkConstraint[part.field])
        } else if req.reverse {
          scanStart[part.field] = part.direction == .asc ? .max : .min
        } else {
          scanStart[part.field] = part.direction == .asc ? .min : .max
        }
      }
      probe = index.comparator.probe(scanStart)
    } else if let startAt {
      probe = { compare($0, startAt) }
    } else {
      probe = nil
    }

    let rows = TreeRows(data, from: probe, reverse: req.reverse)

    // Fast path: no overlay, no start, and no filters.
    let overlayActive = overlay.map { conn.lastPushedEpoch >= $0.epoch } ?? false
    if !overlayActive && req.start == nil && conn.filters == nil && req.filter == nil {
      return AnyIterator(ScanIterator(rows: rows, constraint: req.constraint))
    }

    // `req.constraint` and not the primary key constraint, which comes from
    // the filters and acts as one.
    let rowsIterable: AnyIterator<Row> =
      if pkConstraint != nil {
        { () -> AnyIterator<Row> in
          var rows = rows
          var taken = false
          return AnyIterator {
            if taken { return nil }
            taken = true
            return rows.next()
          }
        }()
      } else {
        AnyIterator(rows)
      }
    let withOverlay = generateWithOverlay(
      startAt: startAt, rows: rowsIterable, constraint: req.constraint, overlay: overlay,
      lastPushedEpoch: conn.lastPushedEpoch, compare: indexComparator,
      startAtCompare: connectionComparator, filterPredicate: mergedFilterPredicate)
    let withStart = generateWithStart(withOverlay, req.start, connectionComparator)
    let withConstraint = generateWithConstraint(withStart, req.constraint)
    if let mergedFilterPredicate {
      return AnyIterator {
        while let node = withConstraint.next() {
          if mergedFilterPredicate(node.row) { return node }
        }
        return nil
      }
    }
    return withConstraint
  }

  private func fetchMulti(_ req: FetchRequest, _ conn: Connection) -> NodeStream {
    let multis = req.multiConstraints!.filter { !$0.isEmpty }
    let primary = multis[0]
    let rest = multis.dropFirst()
    let base = req.constraint
    let subStreams: [NodeStream] = primary.map { c in
      var sub = req
      sub.constraint = base.map { $0.merging(c) } ?? c
      sub.multiConstraints = nil
      return fetch(sub, conn)
    }
    let compareRows = conn.compareRows
    let merged = mergeSortedStreams(
      subStreams,
      req.reverse ? { compareRows($1.row, $0.row) } : { compareRows($0.row, $1.row) })
    if rest.isEmpty { return merged }
    return NodeStream {
      var it = merged.makeIterator()
      return AnyIterator {
        while let node = it.next() {
          if rest.allSatisfy({ mc in mc.contains { $0.matches(node.row) } }) { return node }
        }
        return nil
      }
    }
  }

  /// Pushes an add for each of `rows`. When nothing is connected and the
  /// source is empty, builds the primary index in one pass instead.
  package func pushAdds(_ rows: [Row]) {
    let rows = rows.map { $0.conformed(to: layout) }
    let index = primaryIndex
    let canBulkLoad = connections.isEmpty && indexes.count == 1 && index.data.isEmpty
    if !canBulkLoad {
      for row in rows { push(.add(row)) }
      return
    }
    let compare = index.comparator.compareRows
    var sorted = rows
    if !isAscending(rows, compare) {
      sorted.sort { compare($0, $1) < 0 }
    }
    indexes[primaryIndexKey]!.data = BTreeSet(sorted: sorted, compare)
  }

  package func push(_ change: SourceChange) {
    genPushAndWriteWithSplitEdit(change.conformed(to: layout))
  }

  func genPushAndWriteWithSplitEdit(_ change: SourceChange) {
    var shouldSplitEdit = false
    if case .edit(let row, let oldRow) = change {
      for conn in connections {
        if let keys = conn.splitEditKeys {
          for key in keys where !ValueCompare.equal(row[key], oldRow[key]) {
            shouldSplitEdit = true
            break
          }
        }
      }
      if shouldSplitEdit {
        pushEpoch += 1
        genPushAndWrite(.remove(oldRow), epoch: pushEpoch)
        pushEpoch += 1
        genPushAndWrite(.add(row), epoch: pushEpoch)
        return
      }
    }
    pushEpoch += 1
    genPushAndWrite(change, epoch: pushEpoch)
  }

  private func genPushAndWrite(_ change: SourceChange, epoch: Int) {
    genPush(change, epoch: epoch)
    writeChange(change)
  }

  private func genPush(_ change: SourceChange, epoch: Int) {
    let data = primaryIndex.data
    switch change {
    case .add(let row):
      precondition(!data.contains(row), "Row already exists \(Self.describe(change))")
    case .remove(let row):
      precondition(data.contains(row), "Row not found \(Self.describe(change))")
    case .edit(_, let oldRow):
      precondition(data.contains(oldRow), "Row not found \(Self.describe(change))")
    }

    overlay = Overlay(epoch: epoch, change: change)
    defer { overlay = nil }
    // Index-based, like iterating the live JS array: a pipeline can
    // disconnect while a push is in flight.
    var i = 0
    while i < connections.count {
      let conn = connections[i]
      i += 1
      guard let output = conn.output else { continue }
      conn.lastPushedEpoch = epoch
      let outputChange: Change =
        switch change {
        case .edit(let row, let oldRow): .edit(Node(row: row), oldNode: Node(row: oldRow))
        case .add(let row): .add(Node(row: row))
        case .remove(let row): .remove(Node(row: row))
        }
      filterPush(outputChange, output, conn.input, conn.filters?.predicate)
    }
    i = 0
    while i < connections.count {
      let conn = connections[i]
      i += 1
      if let output = conn.output {
        output.reconcile(pusher: conn.input)
      }
    }
  }

  private func writeChange(_ change: SourceChange) {
    for key in indexOrder {
      switch change {
      case .add(let row):
        indexes[key]!.data.add(row)
      case .remove(let row):
        let removed = indexes[key]!.data.delete(row)
        precondition(removed, "MemorySource: remove must succeed")
      case .edit(let row, let oldRow):
        let removed = indexes[key]!.data.delete(oldRow)
        precondition(removed, "MemorySource: edit remove must succeed")
        indexes[key]!.data.add(row)
      }
    }
  }

  static func describe(_ change: SourceChange) -> String {
    switch change {
    case .add(let r): "[0,\(JSONSerializer.stringify(JSONValue(r))),null]"
    case .remove(let r): "[1,\(JSONSerializer.stringify(JSONValue(r))),null]"
    case .edit(let r, let o):
      "[2,\(JSONSerializer.stringify(JSONValue(r))),\(JSONSerializer.stringify(JSONValue(o)))]"
    }
  }
}

/// A pipeline's connection to a `MemorySource`.
final class MemorySourceInput: SourceInput {
  /// Weak: a view can outlive the client and its sources.
  weak var source: MemorySource?
  let connection: MemorySource.Connection
  let schema: SourceSchema
  let fullyAppliedFilters: Bool

  init(
    source: MemorySource, connection: MemorySource.Connection, schema: SourceSchema,
    fullyAppliedFilters: Bool
  ) {
    self.source = source
    self.connection = connection
    self.schema = schema
    self.fullyAppliedFilters = fullyAppliedFilters
  }

  func getSchema() -> SourceSchema { schema }
  func fetch(_ req: FetchRequest) -> NodeStream { source?.fetch(req, connection) ?? .empty }
  func setOutput(_ output: any Output) { connection.output = output }
  func destroy() { source?.disconnect(self) }
}

/// A bound in an index scan: a value, or below or above every value.
enum Bound {
  case value(Value?)
  case min
  case max
}

/// `makeBoundComparator(sort)`: orders rows by `sort`, and rows against
/// partial rows whose fields may be `min` or `max`. Rows in `layout` read
/// the columns by index.
struct BoundComparator {
  let sort: AST.Ordering
  let layout: RowLayout
  let compareRows: Comparator

  init(_ sort: AST.Ordering, layout: RowLayout) {
    self.sort = sort
    self.layout = layout
    compareRows = makeComparator(sort, layout: layout)
  }

  /// Compares rows to `bound`: negative when the row is before it.
  func probe(_ bound: [String: Bound]) -> (Row) -> Int {
    let parts = sort.map {
      (layout.column($0.field), $0.direction == .asc, bound[$0.field] ?? .value(nil))
    }
    return { row in
      for (field, asc, b) in parts {
        let c = compareBounds(.value(row[column: field]), b)
        if c != 0 { return asc ? c : -c }
      }
      return 0
    }
  }
}

func compareBounds(_ a: Bound, _ b: Bound) -> Int {
  switch (a, b) {
  case (.min, .min), (.max, .max): return 0
  case (.min, _): return -1
  case (.max, _): return 1
  case (_, .min): return 1
  case (_, .max): return -1
  case (.value(let x), .value(let y)): return ValueCompare.compare(x, y)
  }
}

func assertOrderingIncludesPK(_ ordering: AST.Ordering, _ pk: [String]) throws(PipelineError) {
  let fields = ordering.map(\.field)
  let missing = pk.filter { !fields.contains($0) }
  if !missing.isEmpty {
    throw PipelineError(
      "Ordering must include all primary key fields. Missing: \(missing.joined(separator: ", ")).")
  }
}

func mergePredicates(_ conn: RowPredicate?, _ reqFilter: AST.Condition?) -> RowPredicate? {
  guard let reqFilter else { return conn }
  // FilterStart's conditions were checked when the pipeline was built.
  let reqPredicate = (try? createPredicate(reqFilter)) ?? { _ in false }
  guard let conn else { return reqPredicate }
  return { conn($0) && reqPredicate($0) }
}

func isAscending(_ rows: [Row], _ compare: Comparator) -> Bool {
  var i = 1
  while i < rows.count {
    if compare(rows[i - 1], rows[i]) > 0 { return false }
    i += 1
  }
  return true
}

func generateWithConstraint(_ it: AnyIterator<Node>, _ constraint: Constraint?) -> AnyIterator<Node>
{
  guard let constraint else { return it }
  var done = false
  return AnyIterator {
    if done { return nil }
    guard let node = it.next() else { return nil }
    if !constraint.matches(node.row) {
      done = true
      return nil
    }
    return node
  }
}

/// `generateWithStart`: skips rows before `start`.
func generateWithStart(
  _ nodes: AnyIterator<Node>, _ start: Start?, _ compare: @escaping Comparator
) -> AnyIterator<Node> {
  guard let start else { return nodes }
  var started = false
  return AnyIterator {
    while let node = nodes.next() {
      if !started {
        let c = compare(node.row, start.row)
        started = start.basis == .at ? c >= 0 : c > 0
      }
      if started { return node }
    }
    return nil
  }
}

/// `generateWithOverlay`: splices the in-flight push into `rows`.
func generateWithOverlay(
  startAt: Row?,
  rows: AnyIterator<Row>,
  constraint: Constraint?,
  overlay: MemorySource.Overlay?,
  lastPushedEpoch: Int,
  compare: @escaping Comparator,
  startAtCompare: Comparator,
  filterPredicate: RowPredicate?,
  multiConstraints: [[Constraint]]? = nil
) -> AnyIterator<Node> {
  var toApply: MemorySource.Overlay?
  if let overlay, lastPushedEpoch >= overlay.epoch { toApply = overlay }
  let overlays = computeOverlays(
    startAt: startAt, constraint: constraint, overlay: toApply, startAtCompare: startAtCompare,
    filterPredicate: filterPredicate, multiConstraints: multiConstraints)
  return generateWithOverlayInner(rows, overlays, compare)
}

func overlaysOf(_ overlay: MemorySource.Overlay?) -> MemorySource.Overlays {
  switch overlay?.change {
  case .add(let row)?: MemorySource.Overlays(add: row, remove: nil)
  case .remove(let row)?: MemorySource.Overlays(add: nil, remove: row)
  case .edit(let row, let oldRow)?: MemorySource.Overlays(add: row, remove: oldRow)
  case nil: MemorySource.Overlays()
  }
}

func computeOverlays(
  startAt: Row?,
  constraint: Constraint?,
  overlay: MemorySource.Overlay?,
  startAtCompare: Comparator,
  filterPredicate: RowPredicate?,
  multiConstraints: [[Constraint]]?
) -> MemorySource.Overlays {
  var overlays = overlaysOf(overlay)
  if let startAt {
    func keep(_ row: Row?) -> Row? {
      guard let row, startAtCompare(row, startAt) >= 0 else { return nil }
      return row
    }
    overlays = MemorySource.Overlays(add: keep(overlays.add), remove: keep(overlays.remove))
  }
  if let constraint {
    overlays = MemorySource.Overlays(
      add: overlays.add.flatMap { constraint.matches($0) ? $0 : nil },
      remove: overlays.remove.flatMap { constraint.matches($0) ? $0 : nil })
  }
  overlays = applyMultiConstraintsToOverlays(overlays, multiConstraints)
  if let filterPredicate {
    overlays = MemorySource.Overlays(
      add: overlays.add.flatMap { filterPredicate($0) ? $0 : nil },
      remove: overlays.remove.flatMap { filterPredicate($0) ? $0 : nil })
  }
  return overlays
}

func applyMultiConstraintsToOverlays(
  _ overlays: MemorySource.Overlays, _ multiConstraints: [[Constraint]]?
) -> MemorySource.Overlays {
  guard let multiConstraints else { return overlays }
  var result = overlays
  for mc in multiConstraints where !mc.isEmpty {
    func matchesAny(_ row: Row?) -> Row? {
      guard let row, mc.contains(where: { $0.matches(row) }) else { return nil }
      return row
    }
    result = MemorySource.Overlays(add: matchesAny(result.add), remove: matchesAny(result.remove))
  }
  return result
}

func generateWithOverlayInner(
  _ rows: AnyIterator<Row>, _ overlays: MemorySource.Overlays, _ compare: @escaping Comparator
) -> AnyIterator<Node> {
  AnyIterator(OverlayIterator(rows: rows, overlays: overlays, compare: compare))
}

/// `generateWithOverlayInner`'s generator. A struct, so that its state isn't
/// variables captured by a closure, each access to which checks exclusivity.
private struct OverlayIterator: IteratorProtocol {
  let rows: AnyIterator<Row>
  let overlays: MemorySource.Overlays
  let compare: Comparator
  var addOverlayYielded = false
  var removeOverlaySkipped = false
  var pending: Row?
  var finished = false

  init(rows: AnyIterator<Row>, overlays: MemorySource.Overlays, compare: @escaping Comparator) {
    self.rows = rows
    self.overlays = overlays
    self.compare = compare
  }

  mutating func next() -> Node? {
    if let p = pending {
      pending = nil
      return Node(row: p)
    }
    if finished { return nil }
    while let row = rows.next() {
      var emitAdd: Row?
      if !addOverlayYielded, let add = overlays.add, compare(add, row) < 0 {
        addOverlayYielded = true
        emitAdd = add
      }
      if !removeOverlaySkipped, let remove = overlays.remove, compare(remove, row) == 0 {
        removeOverlaySkipped = true
        if let emitAdd { return Node(row: emitAdd) }
        continue
      }
      if let emitAdd {
        pending = row
        return Node(row: emitAdd)
      }
      return Node(row: row)
    }
    finished = true
    if !addOverlayYielded, let add = overlays.add {
      addOverlayYielded = true
      return Node(row: add)
    }
    return nil
  }
}

/// `mergeSortedStreams`: an N-way merge of streams sorted by `compare`.
func mergeSortedStreams(
  _ streams: [NodeStream], _ compare: @escaping (Node, Node) -> Int
)
  -> NodeStream
{
  NodeStream {
    var iterators = streams.map { $0.makeIterator() }
    var heap: [(node: Node, idx: Int)] = []
    var primed = false

    func siftUp(_ start: Int) {
      var i = start
      while i > 0 {
        let p = (i - 1) >> 1
        if compare(heap[i].node, heap[p].node) >= 0 { return }
        heap.swapAt(i, p)
        i = p
      }
    }
    func siftDown(_ start: Int) {
      var i = start
      let n = heap.count
      while true {
        let l = (i << 1) + 1
        let r = l + 1
        var smallest = i
        if l < n && compare(heap[l].node, heap[smallest].node) < 0 { smallest = l }
        if r < n && compare(heap[r].node, heap[smallest].node) < 0 { smallest = r }
        if smallest == i { return }
        heap.swapAt(i, smallest)
        i = smallest
      }
    }

    var pendingRefill: Int?
    return AnyIterator {
      if !primed {
        primed = true
        for i in 0..<iterators.count {
          if let node = iterators[i].next() {
            heap.append((node, i))
            siftUp(heap.count - 1)
          }
        }
      }
      // Refill from the stream of the node emitted last, now that the
      // consumer asked for the next one (as the JS generator does on resume).
      if let idx = pendingRefill {
        pendingRefill = nil
        if let next = iterators[idx].next() {
          heap[0] = (next, idx)
          siftDown(0)
        } else {
          let last = heap.removeLast()
          if !heap.isEmpty {
            heap[0] = last
            siftDown(0)
          }
        }
      }
      guard let top = heap.first else { return nil }
      pendingRefill = top.idx
      return top.node
    }
  }
}

/// A tree's rows, ascending or descending. A struct rather than an
/// `AnyIterator` of the tree's iterator, which would call its unspecialized
/// `next()`, or a closure capturing it, each call of which checks exclusivity.
private struct TreeRows: IteratorProtocol {
  var forward: BTreeSet<Row>.ForwardIterator?
  var reverse: BTreeSet<Row>.ReverseIterator?

  init(_ data: BTreeSet<Row>, from probe: ((Row) -> Int)?, reverse: Bool) {
    if reverse {
      self.reverse = data.valuesReversed(from: probe)
    } else {
      forward = data.values(from: probe)
    }
  }

  mutating func next() -> Row? {
    forward != nil ? forward!.next() : reverse!.next()
  }
}

/// The rows of a fetch without overlay, start, or filters, up to the first
/// that doesn't match `constraint`.
private struct ScanIterator: IteratorProtocol {
  var rows: TreeRows
  let constraint: Constraint?
  var done = false

  mutating func next() -> Node? {
    if done { return nil }
    guard let row = rows.next() else { return nil }
    if let constraint, !constraint.matches(row) {
      done = true
      return nil
    }
    return Node(row: row)
  }
}
