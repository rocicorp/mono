/// Implements `limit`: passes the first `limit` nodes of its input and keeps
/// a bound, the last accepted row, to decide on pushes (`take.ts`). Counts
/// globally or per value of a partition key.
final class Take: Operator, TakeBoundProvider {
  struct State {
    var size: Int
    var bound: Row?
  }

  struct DirtyPartition {
    var constraint: Constraint?
    /// The bound reported to an upstream `TakeGate` until reconcile, so a
    /// removal does not unbound the gate during the push.
    var gateBound: Row?
  }

  let input: any Input
  let limit: Int
  let partitionKey: [String]?
  var storage: [JSKey: State] = [:]
  /// A row hidden from fetches while a split push is in flight.
  var rowHiddenFromFetch: Row?
  weak var takeGate: TakeGate?
  /// Partitions with removals in phase one that reconcile must refill, in
  /// insertion order.
  var dirtyPartitions: [(key: JSKey, state: DirtyPartition)] = []
  weak var output: (any Output)?

  init(_ input: any Input, limit: Int, partitionKey: [String]? = nil) throws(PipelineError) {
    precondition(limit >= 0, "Limit must be non-negative")
    let schema = input.getSchema()
    guard let sort = schema.sort else { throw PipelineError("Take requires sorted input") }
    try assertOrderingIncludesPK(sort, schema.primaryKey)
    self.input = input
    self.limit = limit
    self.partitionKey = partitionKey
    input.setOutput(self)
  }

  func setTakeGate(_ gate: TakeGate) { takeGate = gate }
  func setOutput(_ output: any Output) { self.output = output }
  func getSchema() -> SourceSchema { input.getSchema() }
  func destroy() { input.destroy() }

  func dirty(_ key: JSKey) -> DirtyPartition? {
    dirtyPartitions.first { $0.key == key }?.state
  }

  func getBound(_ constraint: Constraint?) -> Row? {
    if let partitionKey, !constraintContainsPartitionKey(constraint, partitionKey) {
      return nil
    }
    let key = takeStateKey(constraint)
    if let dirty = dirty(key) {
      // Phase one: keep upstream gates at the bound before the removals.
      return dirty.gateBound
    }
    guard let state = storage[key], state.size >= limit else { return nil }
    return state.bound
  }

  func fetch(_ req: FetchRequest) -> NodeStream {
    NodeStream { [self] in
      precondition(
        partitionKey == nil
          || (req.constraint != nil
            && constraintContainsPartitionKey(req.constraint, partitionKey)),
        "Partitioned take does not allow unpartitioned fetches")
      let key = takeStateKey(req.constraint)
      guard let state = storage[key] else {
        if constraintMatchesPartitionKey(req.constraint, partitionKey) {
          return initialFetch(req, key)
        }
        return AnyIterator { nil }
      }
      guard let bound = state.bound else { return AnyIterator { nil } }
      var it: LazyIterator<Node>? = input.fetch(req).makeIterator()
      var count = 0
      var yielded = false
      let compareRows = getSchema().compareRows
      return AnyIterator {
        // Count the node yielded last on resume, as the JS generator does.
        if yielded {
          yielded = false
          count += 1
          if count >= self.limit { it = nil }
        }
        while let node = it?.next() {
          if compareRows(bound, node.row) < 0 { break }
          if let hidden = self.rowHiddenFromFetch, compareRows(hidden, node.row) == 0 { continue }
          yielded = true
          return node
        }
        it = nil
        return nil
      }
    }
  }

  /// The first fetch of a partition: hydrates its state. The state is
  /// written once the stream ends, as in the JS `finally`.
  private func initialFetch(_ req: FetchRequest, _ key: JSKey) -> AnyIterator<Node> {
    precondition(req.start == nil, "Start should be undefined")
    precondition(!req.reverse, "Reverse should be false")
    if limit == 0 { return AnyIterator { nil } }
    var it: LazyIterator<Node>? = input.fetch(req).makeIterator()
    var size = 0
    var bound: Row?
    var pending: Node?
    return AnyIterator { [self] in
      guard it != nil else { return nil }
      if let p = pending {
        pending = nil
        bound = p.row
        size += 1
        if size == limit {
          it = nil
          storage[key] = State(size: size, bound: bound)
          return nil
        }
      }
      guard let node = it!.next() else {
        it = nil
        storage[key] = State(size: size, bound: bound)
        return nil
      }
      pending = node
      return node
    }
  }

  func stateAndConstraint(_ row: Row) -> (State?, JSKey, Constraint?) {
    let key = takeStateKey(row)
    let state = storage[key]
    var constraint: Constraint?
    if state != nil, let partitionKey {
      constraint = Constraint.from(row, partitionKey)
    }
    return (state, key, constraint)
  }

  func push(_ change: Change, pusher: any InputBase) {
    if case .edit(let node, let oldNode) = change {
      pushEditChange(change, node, oldNode)
      return
    }
    let (maybeState, key, constraint) = stateAndConstraint(change.node.row)
    guard let state = maybeState else { return }
    let compareRows = getSchema().compareRows
    switch change {
    case .add(let node):
      if state.size < limit {
        if dirty(key) != nil {
          // With an unresolved deficit, only admit rows before the bound, so
          // reconcile can backfill earlier rows in order.
          if state.bound == nil || compareRows(node.row, state.bound!) >= 0 { return }
        }
        let nextSize = state.size + 1
        if nextSize == limit { dirtyPartitions.removeAll { $0.key == key } }
        setState(
          key, nextSize,
          state.bound == nil || compareRows(state.bound!, node.row) < 0 ? node.row : state.bound)
        output?.push(change, pusher: self)
        return
      }
      // size == limit
      guard let bound = state.bound, compareRows(node.row, bound) < 0 else { return }
      var beforeBoundNode: Node?
      var boundNode: Node?
      if limit == 1 {
        boundNode =
          input.fetch(
            FetchRequest(constraint: constraint, start: Start(row: bound, basis: .at))
          ).firstElement
      } else {
        var it = input.fetch(
          FetchRequest(constraint: constraint, start: Start(row: bound, basis: .at), reverse: true)
        ).makeIterator()
        boundNode = it.next()
        beforeBoundNode = it.next()
      }
      guard let boundNode else { preconditionFailure("Take: boundNode must be found during fetch") }
      // Remove before add to keep output size <= limit.
      setState(
        key, state.size,
        beforeBoundNode == nil || compareRows(node.row, beforeBoundNode!.row) > 0
          ? node.row : beforeBoundNode!.row)
      pushWithRowHiddenFromFetch(node.row, .remove(boundNode))
      output?.push(change, pusher: self)
    case .remove(let node):
      guard let bound = state.bound else { return }  // after the bound
      let compToBound = compareRows(node.row, bound)
      if compToBound > 0 || (compToBound < 0 && limit == 1) { return }
      let beforeBoundNode = input.fetch(
        FetchRequest(constraint: constraint, start: Start(row: bound, basis: .after), reverse: true)
      ).firstElement
      let finalBound: Row? =
        state.size - 1 == 0 ? nil : compToBound < 0 ? bound : beforeBoundNode?.row
      if dirty(key) == nil {
        // Snapshot the bound before the first removal for upstream gates.
        dirtyPartitions.append((key, DirtyPartition(constraint: constraint, gateBound: bound)))
      }
      setState(key, state.size - 1, finalBound)
      output?.push(change, pusher: self)
    case .child(let node, _):
      if let bound = state.bound, compareRows(node.row, bound) <= 0 {
        output?.push(change, pusher: self)
      }
    case .edit:
      break
    }
  }

  private func pushEditChange(_ change: Change, _ node: Node, _ oldNode: Node) {
    if let partitionKey {
      precondition(
        rowEqualsForCompoundKey(oldNode.row, node.row, partitionKey),
        "Unexpected change of partition key")
    }
    let (maybeState, key, constraint) = stateAndConstraint(oldNode.row)
    guard let state = maybeState else { return }
    guard let bound = state.bound else { preconditionFailure("Bound should be set") }
    let compareRows = getSchema().compareRows
    let oldCmp = compareRows(oldNode.row, bound)
    let newCmp = compareRows(node.row, bound)

    func replaceBoundAndForwardChange() {
      setState(key, state.size, node.row)
      output?.push(change, pusher: self)
    }

    if oldCmp == 0 {
      // The bound row changed.
      if newCmp == 0 {
        output?.push(change, pusher: self)
        return
      }
      if newCmp < 0 {
        if limit == 1 {
          replaceBoundAndForwardChange()
          return
        }
        // The new row stays in, but might no longer be the bound.
        guard
          let beforeBoundNode = input.fetch(
            FetchRequest(
              constraint: constraint, start: Start(row: bound, basis: .after), reverse: true)
          ).firstElement
        else { preconditionFailure("Take: beforeBoundNode must be found during fetch") }
        setState(key, state.size, beforeBoundNode.row)
        output?.push(change, pusher: self)
        return
      }
      precondition(newCmp > 0, "New comparison must be greater than 0")
      // The first row at the old bound is the new bound.
      takeGate?.open()
      let newBoundNode = input.fetch(
        FetchRequest(constraint: constraint, start: Start(row: bound, basis: .at))
      ).firstElement
      takeGate?.close()
      guard let newBoundNode else {
        preconditionFailure("Take: newBoundNode must be found during fetch")
      }
      if compareRows(newBoundNode.row, node.row) == 0 {
        replaceBoundAndForwardChange()
        return
      }
      // The new row is out of bounds: remove the old row, add the new bound.
      setState(key, state.size, newBoundNode.row)
      pushWithRowHiddenFromFetch(newBoundNode.row, .remove(oldNode))
      output?.push(.add(newBoundNode), pusher: self)
      return
    }

    if oldCmp > 0 {
      precondition(newCmp != 0, "Invalid state. Row has duplicate primary key")
      if newCmp > 0 { return }  // both outside
      // Old was outside, new is inside: push out the old bound.
      var it = input.fetch(
        FetchRequest(constraint: constraint, start: Start(row: bound, basis: .at), reverse: true)
      ).makeIterator()
      guard let oldBoundNode = it.next() else {
        preconditionFailure("Take: oldBoundNode must be found during fetch")
      }
      guard let newBoundNode = it.next() else {
        preconditionFailure("Take: newBoundNode must be found during fetch")
      }
      setState(key, state.size, newBoundNode.row)
      pushWithRowHiddenFromFetch(node.row, .remove(oldBoundNode))
      output?.push(.add(node), pusher: self)
      return
    }

    // oldCmp < 0
    precondition(newCmp != 0, "Invalid state. Row has duplicate primary key")
    if newCmp < 0 {
      output?.push(change, pusher: self)  // both inside
      return
    }
    // Old was inside, new is past the bound: the row after the bound or the
    // new row is the new bound.
    takeGate?.open()
    let afterBoundNode = input.fetch(
      FetchRequest(constraint: constraint, start: Start(row: bound, basis: .after))
    ).firstElement
    takeGate?.close()
    guard let afterBoundNode else {
      preconditionFailure("Take: afterBoundNode must be found during fetch")
    }
    if compareRows(afterBoundNode.row, node.row) == 0 {
      replaceBoundAndForwardChange()
      return
    }
    output?.push(.remove(oldNode), pusher: self)
    setState(key, state.size, afterBoundNode.row)
    output?.push(.add(afterBoundNode), pusher: self)
  }

  private func pushWithRowHiddenFromFetch(_ row: Row, _ change: Change) {
    rowHiddenFromFetch = row
    defer { rowHiddenFromFetch = nil }
    output?.push(change, pusher: self)
  }

  private func setState(_ key: JSKey, _ size: Int, _ bound: Row?) {
    storage[key] = State(size: size, bound: bound)
  }

  func reconcile(pusher: any InputBase) {
    if !dirtyPartitions.isEmpty {
      let dirty = dirtyPartitions
      dirtyPartitions.removeAll()
      for (key, d) in dirty {
        guard let state = storage[key] else {
          preconditionFailure("Take: dirty partition must exist in storage")
        }
        let deficit = limit - state.size
        precondition(deficit > 0, "Take: dirty partition must have a deficit")
        var toPush: [Node] = []
        takeGate?.open()
        do {
          var it = input.fetch(
            FetchRequest(
              constraint: d.constraint, start: state.bound.map { Start(row: $0, basis: .after) })
          ).makeIterator()
          while let node = it.next() {
            toPush.append(node)
            if toPush.count == deficit { break }
          }
        }
        takeGate?.close()
        var currentSize = state.size
        for node in toPush {
          currentSize += 1
          setState(key, currentSize, node.row)
          output?.push(.add(node), pusher: self)
        }
      }
    }
    output?.reconcile(pusher: self)
  }

  func takeStateKey(_ row: Row) -> JSKey {
    var values: [JSONValue] = [.string("take")]
    if let partitionKey {
      for k in partitionKey { values.append(row[k] ?? .null) }
    }
    return JSKey(JSONSerializer.stringify(.array(values)))
  }

  func takeStateKey(_ constraint: Constraint?) -> JSKey {
    var values: [JSONValue] = [.string("take")]
    if let partitionKey, let constraint {
      for k in partitionKey { values.append(constraint[k] ?? .null) }
    }
    return JSKey(JSONSerializer.stringify(.array(values)))
  }
}

func constraintMatchesPartitionKey(_ constraint: Constraint?, _ partitionKey: [String]?) -> Bool {
  guard let constraint, let partitionKey else { return constraint == nil && partitionKey == nil }
  if partitionKey.count != constraint.count { return false }
  return partitionKey.allSatisfy { constraint.has($0) }
}

func constraintContainsPartitionKey(_ constraint: Constraint?, _ partitionKey: [String]?) -> Bool {
  guard let constraint, let partitionKey else { return false }
  return partitionKey.allSatisfy { constraint.has($0) }
}

/// Sets where the pipeline starts: no row before the bound is output
/// (`skip.ts`).
final class Skip: Operator {
  let input: any Input
  let bound: AST.Bound
  /// The bound's row.
  let boundRow: Row
  let comparator: Comparator
  weak var output: (any Output)?

  init(_ input: any Input, _ bound: AST.Bound) {
    let schema = input.getSchema()
    precondition(schema.sort != nil, "Skip requires sorted input")
    self.input = input
    self.bound = bound
    boundRow = Row(bound.row)
    comparator = schema.compareRows
    input.setOutput(self)
  }

  func getSchema() -> SourceSchema { input.getSchema() }
  func setOutput(_ output: any Output) { self.output = output }
  func destroy() { input.destroy() }

  func fetch(_ req: FetchRequest) -> NodeStream {
    NodeStream { [self] in
      guard case .start(let start) = getStart(req) else { return AnyIterator { nil } }
      var r = req
      r.start = start
      var it: LazyIterator<Node>? = input.fetch(r).makeIterator()
      if !req.reverse { return AnyIterator { it?.next() } }
      return AnyIterator {
        guard let node = it?.next() else { return nil }
        if !self.shouldBePresent(node.row) {
          it = nil
          return nil
        }
        return node
      }
    }
  }

  func shouldBePresent(_ row: Row) -> Bool {
    let cmp = comparator(boundRow, row)
    return cmp < 0 || (cmp == 0 && !bound.exclusive)
  }

  func push(_ change: Change, pusher: any InputBase) {
    if case .edit(let node, let oldNode) = change {
      maybeSplitAndPushEditChange(node, oldNode, shouldBePresent, output, self)
      return
    }
    if shouldBePresent(change.node.row) { output?.push(change, pusher: self) }
  }

  func reconcile(pusher: any InputBase) { output?.reconcile(pusher: self) }

  enum FetchStart {
    case empty
    case start(Start?)
  }

  private func getStart(_ req: FetchRequest) -> FetchStart {
    let boundStart = Start(row: boundRow, basis: bound.exclusive ? .after : .at)
    guard let reqStart = req.start else {
      return req.reverse ? .start(nil) : .start(boundStart)
    }
    let cmp = comparator(boundRow, reqStart.row)
    if !req.reverse {
      // The skip bound is after the requested start.
      if cmp > 0 { return .start(boundStart) }
      if cmp == 0 {
        if bound.exclusive || reqStart.basis == .after {
          return .start(Start(row: boundRow, basis: .after))
        }
        return .start(boundStart)
      }
      return .start(reqStart)
    }
    // Reverse: a bound after the start leaves nothing.
    if cmp > 0 { return .empty }
    if cmp == 0 {
      if !bound.exclusive && reqStart.basis == .at { return .start(boundStart) }
      return .empty
    }
    return .start(reqStart)
  }
}

/// A count-based limit for EXISTS subqueries that needs no order: it tracks
/// membership by primary key (`cap.ts`).
final class Cap: Operator {
  struct State {
    var size: Int
    var pks: [JSKey]
  }

  let input: any Input
  let limit: Int
  let partitionKey: [String]?
  let primaryKey: [String]
  var storage: [JSKey: State] = [:]
  weak var output: (any Output)?

  init(_ input: any Input, limit: Int, partitionKey: [String]? = nil) {
    precondition(limit >= 0, "Limit must be non-negative")
    self.input = input
    self.limit = limit
    self.partitionKey = partitionKey
    primaryKey = input.getSchema().primaryKey
    input.setOutput(self)
  }

  func setOutput(_ output: any Output) { self.output = output }
  func getSchema() -> SourceSchema { input.getSchema() }
  func destroy() { input.destroy() }
  func reconcile(pusher: any InputBase) { output?.reconcile(pusher: self) }

  func fetch(_ req: FetchRequest) -> NodeStream {
    NodeStream { [self] in
      precondition(req.start == nil, "Cap does not support start")
      precondition(!req.reverse, "Cap does not support reverse")
      precondition(
        partitionKey == nil
          || (req.constraint != nil && constraintMatchesPartitionKey(req.constraint, partitionKey)),
        "Cap fetch: constraint must match partition key when partitioned")
      let key = capStateKey(req.constraint)
      guard let state = storage[key] else { return initialFetch(req, key) }
      if state.size == 0 { return AnyIterator { nil } }
      // Point lookups by the tracked primary keys.
      var pkIndex = 0
      var current: LazyIterator<Node>?
      return AnyIterator {
        while true {
          if let node = current?.next() { return node }
          guard pkIndex < state.pks.count else { return nil }
          let pkConstraint = self.deserializePK(state.pks[pkIndex])
          pkIndex += 1
          // Keep the partition key so partitioned operators upstream keep
          // their scope.
          let constraint = req.constraint.map { $0.merging(pkConstraint) } ?? pkConstraint
          current = self.input.fetch(FetchRequest(constraint: constraint)).makeIterator()
        }
      }
    }
  }

  private func initialFetch(_ req: FetchRequest, _ key: JSKey) -> AnyIterator<Node> {
    if limit == 0 { return AnyIterator { nil } }
    precondition(
      constraintMatchesPartitionKey(req.constraint, partitionKey),
      "Constraint should match partition key")
    var it: LazyIterator<Node>? = input.fetch(req).makeIterator()
    var size = 0
    var pks: [JSKey] = []
    var pending: Node?
    return AnyIterator { [self] in
      guard it != nil else { return nil }
      if let p = pending {
        pending = nil
        pks.append(serializePK(p.row))
        size += 1
        if size == limit {
          it = nil
          storage[key] = State(size: size, pks: pks)
          return nil
        }
      }
      guard let node = it!.next() else {
        it = nil
        storage[key] = State(size: size, pks: pks)
        return nil
      }
      pending = node
      return node
    }
  }

  func push(_ change: Change, pusher: any InputBase) {
    if case .edit(let node, let oldNode) = change {
      if let partitionKey {
        precondition(
          rowEqualsForCompoundKey(oldNode.row, node.row, partitionKey),
          "Unexpected change of partition key")
      }
      let key = capStateKey(oldNode.row)
      guard let state = storage[key] else { return }
      let oldPK = serializePK(oldNode.row)
      if state.pks.contains(oldPK) {
        let newPK = serializePK(node.row)
        if oldPK != newPK {
          storage[key] = State(size: state.size, pks: state.pks.map { $0 == oldPK ? newPK : $0 })
        }
        output?.push(change, pusher: self)
      }
      return
    }
    let key = capStateKey(change.node.row)
    guard let state = storage[key] else { return }
    let pk = serializePK(change.node.row)
    switch change {
    case .add:
      if state.size < limit {
        storage[key] = State(size: state.size + 1, pks: state.pks + [pk])
        output?.push(change, pusher: self)
      }
    case .remove(let node):
      guard let pkIndex = state.pks.firstIndex(of: pk) else { return }
      var pks = state.pks
      pks.remove(at: pkIndex)
      let newSize = state.size - 1
      // Refill with the first row not in the set.
      let pkSet = Set(pks)
      let constraint = partitionKey.map { Constraint.from(node.row, $0) }
      var replacement: Node?
      for candidate in input.fetch(FetchRequest(constraint: constraint))
      where !pkSet.contains(serializePK(candidate.row)) {
        replacement = candidate
        break
      }
      if let replacement {
        // Without the replacement while the remove is forwarded.
        storage[key] = State(size: newSize, pks: pks)
        output?.push(change, pusher: self)
        pks.append(serializePK(replacement.row))
        storage[key] = State(size: newSize + 1, pks: pks)
        output?.push(.add(replacement), pusher: self)
      } else {
        storage[key] = State(size: newSize, pks: pks)
        output?.push(change, pusher: self)
      }
    case .child:
      if state.pks.contains(pk) { output?.push(change, pusher: self) }
    case .edit:
      break
    }
  }

  func capStateKey(_ row: Row) -> JSKey {
    var values: [JSONValue] = [.string("cap")]
    if let partitionKey { for k in partitionKey { values.append(row[k] ?? .null) } }
    return JSKey(JSONSerializer.stringify(.array(values)))
  }

  func capStateKey(_ constraint: Constraint?) -> JSKey {
    var values: [JSONValue] = [.string("cap")]
    if let partitionKey, let constraint {
      for k in partitionKey { values.append(constraint[k] ?? .null) }
    }
    return JSKey(JSONSerializer.stringify(.array(values)))
  }

  func serializePK(_ row: Row) -> JSKey {
    JSKey(JSONSerializer.stringify(.array(primaryKey.map { row[$0] ?? .null })))
  }

  func deserializePK(_ pk: JSKey) -> Constraint {
    let values = (try? JSONParser.parse(pk.string))?.arrayValue ?? []
    var c = Constraint()
    for (i, k) in primaryKey.enumerated() { c.set(k, i < values.count ? values[i] : nil) }
    return c
  }
}
