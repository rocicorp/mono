/// Forks a pipeline for an OR with flipped subqueries (`union-fan-out.ts`).
final class UnionFanOut: Operator {
  var destroyCount = 0
  weak var unionFanIn: UnionFanIn?
  let input: any Input
  var outputs: [WeakOutput] = []

  init(_ input: any Input) {
    self.input = input
    input.setOutput(self)
  }

  func setFanIn(_ fanIn: UnionFanIn) {
    precondition(unionFanIn == nil, "FanIn already set for this FanOut")
    unionFanIn = fanIn
  }

  func push(_ change: Change, pusher: any InputBase) {
    guard let fanIn = unionFanIn else { preconditionFailure("UnionFanOut has no fan-in") }
    fanIn.fanOutStartedPushing()
    for o in outputs { o.value?.push(change, pusher: self) }
    fanIn.fanOutDonePushing(change.type)
  }

  func reconcile(pusher: any InputBase) {
    guard let fanIn = unionFanIn else { preconditionFailure("UnionFanOut has no fan-in") }
    fanIn.fanOutStartedReconciling()
    for o in outputs { o.value?.reconcile(pusher: self) }
    fanIn.fanOutDoneReconciling()
  }

  func setOutput(_ output: any Output) { outputs.append(WeakOutput(output)) }
  func getSchema() -> SourceSchema { input.getSchema() }
  func fetch(_ req: FetchRequest) -> NodeStream { input.fetch(req) }

  func destroy() {
    precondition(destroyCount < outputs.count, "FanOut already destroyed once for each output")
    destroyCount += 1
    if destroyCount == outputs.count { input.destroy() }
  }
}

/// Merges the branches of a `UnionFanOut` (`union-fan-in.ts`).
final class UnionFanIn: Operator {
  let inputs: [any Input]
  let schema: SourceSchema
  var fanOutPushStarted = false
  var fanOutReconcileStarted = false
  weak var output: (any Output)?
  var accumulatedPushes: [Change] = []
  /// Relationships the branches add (flipped joins' witnesses).
  let branchRels: Set<String>

  init(_ fanOut: UnionFanOut, _ inputs: [any Input]) {
    self.inputs = inputs
    let fanOutSchema = fanOut.getSchema()
    precondition(fanOutSchema.sort != nil, "UnionFanIn requires sorted input")
    var relationships = fanOutSchema.relationships
    var fromBranches: Set<String> = []
    for input in inputs {
      let inputSchema = input.getSchema()
      precondition(
        fanOutSchema.tableName == inputSchema.tableName,
        "Table name mismatch in union fan-in: \(fanOutSchema.tableName) !== \(inputSchema.tableName)"
      )
      precondition(
        fanOutSchema.primaryKey == inputSchema.primaryKey, "Primary key mismatch in union fan-in")
      precondition(fanOutSchema.system == inputSchema.system, "System mismatch in union fan-in")
      for (name, relSchema) in inputSchema.relationships {
        if fanOutSchema.relationship(name) != nil { continue }
        precondition(
          !fromBranches.contains(name),
          "Relationship \(name) exists in multiple upstream inputs to union fan-in")
        relationships.append((name, relSchema))
        fromBranches.insert(name)
      }
    }
    schema = fanOutSchema.with(relationships: relationships)
    branchRels = fromBranches
    fanOut.setFanIn(self)
    for input in inputs { input.setOutput(self) }
  }

  func destroy() { for i in inputs { i.destroy() } }
  func getSchema() -> SourceSchema { schema }
  func setOutput(_ output: any Output) { self.output = output }

  func fetch(_ req: FetchRequest) -> NodeStream {
    let streams = inputs.map { $0.fetch(req) }
    let compareRows = schema.compareRows
    let compare: (Node, Node) -> Int =
      req.reverse ? { compareRows($1.row, $0.row) } : { compareRows($0.row, $1.row) }
    let relNames = schema.relationshipNames
    return mergeFetches(streams, compare).mapLazy { node in
      var relationships = node.relationships
      mergeEmpty(&relationships, relNames)
      return Node(row: node.row, relationships: relationships)
    }
  }

  func push(_ change: Change, pusher: any InputBase) {
    if !fanOutPushStarted {
      pushInternalChange(change, pusher)
      return
    }
    accumulatedPushes.append(change)
  }

  /// A change from inside the fan-out/fan-in sub-graph, which only a
  /// flipped join's child produces. An add or remove is forwarded when no
  /// other branch has the row.
  private func pushInternalChange(_ change: Change, _ pusher: any InputBase) {
    if change.type == .child {
      output?.push(change, pusher: self)
      return
    }
    precondition(
      change.type == .add || change.type == .remove,
      "UnionFanIn: expected add or remove change type, got \(change.type.rawValue)")
    var hadMatch = false
    for input in inputs {
      if input === pusher {
        hadMatch = true
        continue
      }
      let constraint = Constraint.from(change.node.row, schema.primaryKey)
      if input.fetch(FetchRequest(constraint: constraint)).firstElement != nil {
        // Another branch keeps the row; it gained or lost this branch's
        // witnesses.
        guard let output else { return }
        pushWitnessDeltas(change.node, change.type, branchRels.contains, output, self)
        return
      }
    }
    precondition(hadMatch, "Pusher was not one of the inputs to union-fan-in!")
    output?.push(change, pusher: self)
  }

  func fanOutStartedPushing() {
    precondition(
      !fanOutPushStarted, "UnionFanIn: fanOutStartedPushing called while already pushing")
    fanOutPushStarted = true
  }

  func fanOutDonePushing(_ fanOutChangeType: ChangeType) {
    precondition(
      fanOutPushStarted, "UnionFanIn: fanOutDonePushing called without fanOutStartedPushing")
    fanOutPushStarted = false
    if inputs.isEmpty || accumulatedPushes.isEmpty { return }
    guard let output else {
      accumulatedPushes.removeAll()
      return
    }
    let schema = self.schema
    pushAccumulatedChanges(
      &accumulatedPushes, output, self, fanOutChangeType,
      mergeRelationships: mergeRelationships,
      addEmptyRelationships: { makeAddEmptyRelationships(schema, $0) },
      isBranchRel: branchRels.contains)
  }

  func fanOutStartedReconciling() {
    precondition(
      !fanOutReconcileStarted,
      "UnionFanIn: fanOutStartedReconciling called while already reconciling")
    fanOutReconcileStarted = true
  }

  func fanOutDoneReconciling() {
    precondition(
      fanOutReconcileStarted,
      "UnionFanIn: fanOutDoneReconciling called without fanOutStartedReconciling")
    fanOutReconcileStarted = false
    if inputs.isEmpty { return }
    output?.reconcile(pusher: self)
  }

  func reconcile(pusher: any InputBase) {
    if fanOutReconcileStarted { return }
    output?.reconcile(pusher: self)
  }
}

/// `mergeFetches`: merges sorted branch streams, uniting the relationships
/// of a row that several branches yield (the first branch wins a conflict).
func mergeFetches(
  _ fetches: [NodeStream], _ comparator: @escaping (Node, Node) -> Int
)
  -> NodeStream
{
  NodeStream {
    var iterators = fetches.map { $0.makeIterator() }
    var current: [Node?] = []
    var initialized = false
    var lastNodeYielded: Node?
    return AnyIterator {
      if !initialized {
        initialized = true
        for i in 0..<iterators.count { current.append(iterators[i].next()) }
      }
      while true {
        var min: (node: Node, index: Int)?
        for (i, c) in current.enumerated() {
          guard let c else { continue }
          if min == nil || comparator(c, min!.node) < 0 { min = (c, i) }
        }
        guard let (minNode, minIndex) = min else { return nil }
        current[minIndex] = iterators[minIndex].next()
        if let last = lastNodeYielded, comparator(last, minNode) == 0 { continue }
        // The same row at the head of another branch: union its
        // relationships in rather than dropping it.
        var merged = minNode
        for j in 0..<iterators.count {
          while let head = current[j], comparator(head, minNode) == 0 {
            merged = Node(
              row: merged.row, relationships: head.relationships.merging(merged.relationships))
            current[j] = iterators[j].next()
          }
        }
        lastNodeYielded = minNode
        return merged
      }
    }
  }
}

/// `pushAccumulatedChanges` (`push-accumulated.ts`): pushes the single
/// change that the changes collected from an OR's branches amount to.
func pushAccumulatedChanges(
  _ accumulatedPushes: inout [Change],
  _ output: any Output,
  _ pusher: any InputBase,
  _ fanOutChangeType: ChangeType,
  mergeRelationships: (Change, Change) -> Change,
  addEmptyRelationships: (Change) -> Change,
  isBranchRel: ((String) -> Bool)? = nil
) {
  if accumulatedPushes.isEmpty { return }
  // Witness changes of a nested union pass through around this fan-in's
  // change: removals before, additions after.
  var witnessesLeft: [Change] = []
  var witnessesEntered: [Change] = []
  // One change per type, in the order the types first appeared.
  var candidates: [(type: ChangeType, change: Change)] = []
  for change in accumulatedPushes {
    if let isBranchRel, case .child(_, let child) = change, isBranchRel(child.relationshipName) {
      if child.change.type == .remove {
        witnessesLeft.append(change)
      } else {
        witnessesEntered.append(change)
      }
      continue
    }
    if fanOutChangeType == .child && change.type != .child {
      precondition(
        !candidates.contains { $0.type == change.type },
        "Fan-in:child expected at most one \(change.type.rawValue) when fan-out is of type child")
    }
    if let i = candidates.firstIndex(where: { $0.type == change.type }) {
      candidates[i].change = mergeRelationships(candidates[i].change, change)
    } else {
      candidates.append((change.type, change))
    }
  }
  accumulatedPushes.removeAll()
  precondition(!candidates.isEmpty, "Fan-in: witness changes without a change for their row")
  for change in witnessesLeft { output.push(change, pusher: pusher) }
  pushCollapsed(
    candidates, output, pusher, fanOutChangeType, mergeRelationships, addEmptyRelationships,
    isBranchRel)
  for change in witnessesEntered { output.push(change, pusher: pusher) }
}

private func pushCollapsed(
  _ candidates: [(type: ChangeType, change: Change)],
  _ output: any Output,
  _ pusher: any InputBase,
  _ fanOutChangeType: ChangeType,
  _ mergeRelationships: (Change, Change) -> Change,
  _ addEmptyRelationships: (Change) -> Change,
  _ isBranchRel: ((String) -> Bool)?
) {
  func get(_ type: ChangeType) -> Change? { candidates.first { $0.type == type }?.change }
  let types = candidates.map(\.type)
  switch fanOutChangeType {
  case .remove:
    precondition(types == [.remove], "Fan-in:remove expected all removes")
    output.push(addEmptyRelationships(get(.remove)!), pusher: pusher)
  case .add:
    precondition(types == [.add], "Fan-in:add expected all adds")
    output.push(addEmptyRelationships(get(.add)!), pusher: pusher)
  case .edit:
    precondition(
      types.allSatisfy { $0 == .add || $0 == .remove || $0 == .edit },
      "Fan-in:edit expected all adds, removes, or edits")
    let addChange = get(.add)
    let removeChange = get(.remove)
    var editChange = get(.edit)
    // The row stays: branches it left take their witnesses, branches it
    // entered bring theirs, as child changes around the edit.
    if let isBranchRel, editChange != nil || (addChange != nil && removeChange != nil) {
      if let removeChange {
        pushWitnessDeltas(removeChange.node, .remove, isBranchRel, output, pusher)
      }
      func strip(_ n: Node) -> Node {
        var r = Relationships()
        for (k, v) in n.relationships where !isBranchRel(k) { r[k] = v }
        return Node(row: n.row, relationships: r)
      }
      output.push(
        addEmptyRelationships(
          editChange ?? .edit(strip(addChange!.node), oldNode: strip(removeChange!.node))),
        pusher: pusher)
      if let addChange {
        pushWitnessDeltas(addChange.node, .add, isBranchRel, output, pusher)
      }
      return
    }
    // An edit supersedes adds and removes.
    if var edit = editChange {
      if let addChange { edit = mergeRelationships(edit, addChange) }
      if let removeChange { edit = mergeRelationships(edit, removeChange) }
      editChange = edit
      output.push(addEmptyRelationships(edit), pusher: pusher)
      return
    }
    // A branch turned the edit into a remove and another into an add.
    if let addChange, let removeChange {
      output.push(
        addEmptyRelationships(.edit(addChange.node, oldNode: removeChange.node)), pusher: pusher)
      return
    }
    output.push(addEmptyRelationships((addChange ?? removeChange)!), pusher: pusher)
  case .child:
    precondition(
      types.allSatisfy { $0 == .add || $0 == .remove || $0 == .child },
      "Fan-in:child expected all adds, removes, or children")
    precondition(
      types.count <= 2, "Fan-in:child expected at most 2 types on a child change from fan-out")
    // A branch that kept the child change takes precedence.
    if let childChange = get(.child) {
      let left = get(.remove)
      let entered = get(.add)
      if let isBranchRel, let left {
        pushWitnessDeltas(left.node, .remove, isBranchRel, output, pusher)
      }
      output.push(childChange, pusher: pusher)
      if let isBranchRel, let entered {
        pushWitnessDeltas(entered.node, .add, isBranchRel, output, pusher)
      }
      return
    }
    let addChange = get(.add)
    let removeChange = get(.remove)
    precondition(
      addChange == nil || removeChange == nil,
      "Fan-in:child expected either add or remove, not both")
    output.push(addEmptyRelationships((addChange ?? removeChange)!), pusher: pusher)
  }
}

/// The row stays, but gained (`add`) or lost (`remove`) every witness in
/// its branch relationships: one child change per witness.
func pushWitnessDeltas(
  _ node: Node, _ type: ChangeType, _ isBranchRel: (String) -> Bool, _ output: any Output,
  _ pusher: any InputBase
) {
  for (name, children) in node.relationships where isBranchRel(name) {
    for child in children() {
      output.push(
        .child(
          node,
          ChildChange(relationshipName: name, change: type == .add ? .add(child) : .remove(child))),
        pusher: pusher)
    }
  }
}

/// `mergeRelationships`: puts `right`'s relationships into `left` where
/// `left` lacks them.
func mergeRelationships(_ left: Change, _ right: Change) -> Change {
  func merged(_ l: Node, _ r: Node) -> Node {
    Node(row: l.row, relationships: r.relationships.merging(l.relationships))
  }
  if left.type == right.type {
    switch (left, right) {
    case (.add(let l), .add(let r)): return .add(merged(l, r))
    case (.remove(let l), .remove(let r)): return .remove(merged(l, r))
    case (.edit(let l, let lo), .edit(let r, let ro)):
      return .edit(merged(l, r), oldNode: merged(lo, ro))
    case (.child(let l, let child), .child(let r, _)):
      return .child(merged(l, r), child)
    default: preconditionFailure()
    }
  }
  guard case .edit(let l, let lo) = left else {
    preconditionFailure(
      "mergeRelationships: when types differ, left.type must be edit, got left.type=\(left.type.rawValue), right.type=\(right.type.rawValue)"
    )
  }
  switch right {
  case .add(let r): return .edit(merged(l, r), oldNode: lo)
  case .remove(let r): return .edit(l, oldNode: merged(lo, r))
  default: preconditionFailure("mergeRelationships: unexpected right change")
  }
}

/// `makeAddEmptyRelationships(schema)(change)`.
func makeAddEmptyRelationships(_ schema: SourceSchema, _ change: Change) -> Change {
  if schema.relationships.isEmpty { return change }
  let names = schema.relationshipNames
  func fill(_ n: Node) -> Node {
    var r = n.relationships
    mergeEmpty(&r, names)
    return Node(row: n.row, relationships: r)
  }
  switch change {
  case .add(let n): return .add(fill(n))
  case .remove(let n): return .remove(fill(n))
  case .edit(let n, let o): return .edit(fill(n), oldNode: fill(o))
  case .child: return change  // only the path to the change has relationships
  }
}

/// Adds an empty stream for each of `names` that `relationships` lacks.
func mergeEmpty(_ relationships: inout Relationships, _ names: [String]) {
  for name in names where relationships[name] == nil {
    relationships[name] = { .empty }
  }
}
