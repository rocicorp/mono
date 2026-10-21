/// Set when a join feeds an EXISTS or NOT EXISTS condition (`ExistsJoin`
/// in `join.ts`). Its parent index is kept by a `JoinIndexTap` at the end
/// of the pipeline.
struct ExistsJoin {
  let op: AST.CorrelatedSubqueryOperator
  let parentIndex: JoinIndex
}

/// Joins a parent and a child input into hierarchical output: each parent
/// node gets a relationship streaming its children (`join.ts`).
final class Join: Input {
  let parent: any Input
  let child: any Input
  let parentKey: [String]
  let childKey: [String]
  let relationshipName: String
  let schema: SourceSchema
  let parentIndex: JoinIndex
  /// Whether this join maintains `parentIndex` itself; not for EXISTS joins.
  let indexesParents: Bool
  /// The child change type still pushed when no indexed parent has its key.
  let qualifyingChildChange: ChangeType?
  let boundProvider: (any TakeBoundProvider)?
  weak var output: (any Output)?
  private var parentOutput: ClosureOutput!
  private var childOutput: ClosureOutput!

  var inprogressChildChange: Change?
  var inprogressChildChangePosition: Row?
  /// Primary keys of the parents the in-progress change reached, kept when
  /// the parent input is unordered.
  var inprogressReachedParents: Set<JSKey>?
  var inprogressParentFetchBounds: [ParentFetchBound]?

  init(
    parent: any Input,
    child: any Input,
    parentKey: [String],
    childKey: [String],
    relationshipName: String,
    hidden: Bool,
    system: AST.System,
    parentPartitionKey: [String]? = nil,
    boundProvider: (any TakeBoundProvider)? = nil,
    exists: ExistsJoin? = nil
  ) {
    precondition(parent !== child, "Parent and child must be different operators")
    precondition(
      parentKey.count == childKey.count, "The parentKey and childKey keys must have same length")
    precondition(exists == nil || parentPartitionKey == nil, "EXISTS joins are not partitioned")
    self.parent = parent
    self.child = child
    self.parentKey = parentKey
    self.childKey = childKey
    self.relationshipName = relationshipName
    if let exists {
      parentIndex = exists.parentIndex
      indexesParents = false
      qualifyingChildChange = exists.op == .exists ? .add : .remove
    } else {
      parentIndex = JoinIndex(parentKey, parent.getSchema().primaryKey, parentPartitionKey)
      indexesParents = true
      qualifyingChildChange = nil
    }
    self.boundProvider = boundProvider
    let parentSchema = parent.getSchema()
    let childSchema = child.getSchema()
    schema = parentSchema.adding(
      relationshipName, childSchema.with(isHidden: hidden, system: system))

    parentOutput = ClosureOutput(
      push: { [unowned self] in self.pushParent($0) },
      reconcile: { [unowned self] in self.output?.reconcile(pusher: self) })
    childOutput = ClosureOutput(
      push: { [unowned self] in self.pushChild($0) },
      reconcile: { [unowned self] in self.output?.reconcile(pusher: self) })
    parent.setOutput(parentOutput)
    child.setOutput(childOutput)
  }

  func destroy() {
    parent.destroy()
    child.destroy()
  }

  func setOutput(_ output: any Output) { self.output = output }
  func getSchema() -> SourceSchema { schema }

  func fetch(_ req: FetchRequest) -> NodeStream {
    NodeStream { [self] in
      var it = parent.fetch(req).makeIterator()
      return AnyIterator {
        guard let parentNode = it.next() else { return nil }
        self.indexParentRow(parentNode.row)
        return self.processParentNode(parentNode.row, parentNode.relationships)
      }
    }
  }

  func pushParent(_ change: Change) {
    switch change {
    case .add(let node):
      indexParentRow(node.row)
      output?.push(.add(processParentNode(node.row, node.relationships)), pusher: self)
    case .remove(let node):
      unindexParentRow(node.row)
      output?.push(.remove(processParentNode(node.row, node.relationships)), pusher: self)
    case .child(let node, let childChange):
      output?.push(
        .child(processParentNode(node.row, node.relationships), childChange), pusher: self)
    case .edit(let node, let oldNode):
      precondition(
        rowEqualsForCompoundKey(oldNode.row, node.row, parentKey),
        "Parent edit must not change relationship.")
      unindexParentRow(oldNode.row)
      indexParentRow(node.row)
      output?.push(
        .edit(
          processParentNode(node.row, node.relationships),
          oldNode: processParentNode(oldNode.row, oldNode.relationships)),
        pusher: self)
    }
  }

  func pushChild(_ change: Change) {
    if case .edit(let node, let oldNode) = change {
      precondition(
        rowEqualsForCompoundKey(oldNode.row, node.row, childKey),
        "Child edit must not change relationship.")
    }
    pushChildChange(change.node.row, change)
  }

  func pushChildChange(_ childRow: Row, _ change: Change) {
    inprogressChildChange = change
    inprogressChildChangePosition = nil
    inprogressReachedParents = parent.getSchema().sort == nil ? [] : nil
    defer {
      inprogressChildChange = nil
      inprogressChildChangePosition = nil
      inprogressReachedParents = nil
      inprogressParentFetchBounds = nil
    }
    guard let constraint = buildJoinConstraint(childRow, childKey, parentKey) else { return }
    let fetchConstraints: [Constraint]
    if let partitions = parentIndex.lookup(childRow, childKey) {
      fetchConstraints = partitions.map { p in p.map { constraint.merging($0) } ?? constraint }
    } else if change.type == qualifyingChildChange {
      fetchConstraints = [constraint]
    } else {
      return
    }
    if let boundProvider {
      inprogressParentFetchBounds = readParentFetchBounds(boundProvider, fetchConstraints)
    }
    let parentNodeStream: NodeStream
    if fetchConstraints.count == 1 {
      parentNodeStream = parent.fetch(FetchRequest(constraint: fetchConstraints[0]))
    } else {
      let compareRows = schema.compareRows
      parentNodeStream = mergeSortedStreams(
        fetchConstraints.map { parent.fetch(FetchRequest(constraint: $0)) },
        { compareRows($0.row, $1.row) })
    }
    for parentNode in parentNodeStream {
      inprogressChildChangePosition = parentNode.row
      inprogressReachedParents?.insert(canonicalKey(parentNode.row, schema.primaryKey))
      let childChange = Change.child(
        processParentNode(parentNode.row, parentNode.relationships),
        ChildChange(relationshipName: relationshipName, change: change))
      output?.push(childChange, pusher: self)
    }
  }

  /// Whether the in-progress child change has yet to reach `row` in the
  /// parent stream.
  func isAfterInprogressPosition(_ row: Row) -> Bool {
    guard let position = inprogressChildChangePosition else { return false }
    if let reached = inprogressReachedParents {
      return !reached.contains(canonicalKey(row, schema.primaryKey))
    }
    return schema.compareRows(row, position) > 0
  }

  func indexParentRow(_ row: Row) {
    if indexesParents { parentIndex.add(row) }
  }

  func unindexParentRow(_ row: Row) {
    if indexesParents { parentIndex.remove(row) }
  }

  func processParentNode(_ parentNodeRow: Row, _ parentNodeRelations: Relationships) -> Node {
    let childStream: () -> NodeStream = { [self] in
      let constraint = buildJoinConstraint(parentNodeRow, parentKey, childKey)
      let stream = constraint.map { child.fetch(FetchRequest(constraint: $0)) } ?? .empty
      // The parent has yet to get the in-progress child change if it comes
      // after the current position and a parent fetch of the push yields it.
      let inPushQueue =
        isAfterInprogressPosition(parentNodeRow)
        && (inprogressParentFetchBounds.map {
          isInParentFetch($0, parentNodeRow, schema.compareRows)
        } ?? true)
      if let inprogress = inprogressChildChange,
        isJoinMatch(parentNodeRow, parentKey, inprogress.node.row, childKey), inPushQueue
      {
        let childSchema = child.getSchema()
        if childSchema.sort == nil {
          return generateWithOverlayUnordered(stream, inprogress, childSchema)
        }
        return generateWithOverlay(stream, inprogress, childSchema)
      }
      return stream
    }
    var relationships = parentNodeRelations
    relationships[relationshipName] = childStream
    return Node(row: parentNodeRow, relationships: relationships)
  }
}

/// Provides `Take`'s bound to upstream operators (`take-gate.ts`).
protocol TakeBoundProvider: AnyObject {
  func getBound(_ constraint: Constraint?) -> Row?
}

/// The bound a `TakeGate` caps one parent fetch of a child push at.
struct ParentFetchBound {
  let constraint: Constraint
  let bound: Row?
}

/// Reads the bounds for a child push's parent fetches before they start.
func readParentFetchBounds(
  _ provider: any TakeBoundProvider, _ constraints: [Constraint]
)
  -> [ParentFetchBound]
{
  constraints.map { ParentFetchBound(constraint: $0, bound: provider.getBound($0)) }
}

/// Whether one of the parent fetches of a push yields `row`.
func isInParentFetch(_ bounds: [ParentFetchBound], _ row: Row, _ compareRows: Comparator) -> Bool {
  for b in bounds where b.constraint.matches(row) {
    guard let bound = b.bound else { return true }
    return compareRows(row, bound) <= 0
  }
  return false
}

/// Caps upstream parent fetches at a downstream `Take`'s bound.
final class TakeGate: Operator, TakeBoundProvider {
  let input: any Input
  let comparator: Comparator
  weak var boundProvider: (any TakeBoundProvider)?
  weak var output: (any Output)?
  var openDepth = 0

  init(_ input: any Input) {
    let schema = input.getSchema()
    precondition(schema.sort != nil, "TakeGate requires sorted input")
    self.input = input
    comparator = schema.compareRows
    input.setOutput(self)
  }

  func open() { openDepth += 1 }

  func close() {
    precondition(openDepth > 0, "TakeGate.close called without matching open")
    openDepth -= 1
  }

  func setBoundProvider(_ provider: any TakeBoundProvider) { boundProvider = provider }

  func getBound(_ constraint: Constraint?) -> Row? {
    if openDepth > 0 { return nil }
    return boundProvider?.getBound(constraint)
  }

  func getSchema() -> SourceSchema { input.getSchema() }
  func setOutput(_ output: any Output) { self.output = output }
  func destroy() { input.destroy() }
  func push(_ change: Change, pusher: any InputBase) { output?.push(change, pusher: self) }
  func reconcile(pusher: any InputBase) { output?.reconcile(pusher: self) }

  func fetch(_ req: FetchRequest) -> NodeStream {
    NodeStream { [self] in
      if openDepth > 0 || req.reverse {
        return AnyIterator(input.fetch(req).makeIterator())
      }
      guard let bound = boundProvider?.getBound(req.constraint) else {
        return AnyIterator(input.fetch(req).makeIterator())
      }
      var it: LazyIterator<Node>? = input.fetch(req).makeIterator()
      let comparator = self.comparator
      return AnyIterator {
        guard let node = it?.next() else { return nil }
        if comparator(node.row, bound) > 0 {
          it = nil
          return nil
        }
        return node
      }
    }
  }
}

/// Keeps the parent indexes of a pipeline's EXISTS joins equal to the rows
/// the pipeline emits (`join-index-tap.ts`).
final class JoinIndexTap: Operator {
  let input: any Input
  let indexes: [JoinIndex]
  weak var output: (any Output)?

  init(_ input: any Input, _ indexes: [JoinIndex]) {
    self.input = input
    self.indexes = indexes
    input.setOutput(self)
  }

  func setOutput(_ output: any Output) { self.output = output }
  func getSchema() -> SourceSchema { input.getSchema() }
  func destroy() { input.destroy() }

  func fetch(_ req: FetchRequest) -> NodeStream {
    let indexes = self.indexes
    return input.fetch(req).mapLazy { node in
      for index in indexes { index.add(node.row) }
      return node
    }
  }

  func push(_ change: Change, pusher: any InputBase) {
    switch change {
    case .add(let node):
      for index in indexes { index.add(node.row) }
    case .remove(let node):
      for index in indexes { index.remove(node.row) }
    case .edit(let node, let oldNode):
      for index in indexes {
        index.remove(oldNode.row)
        index.add(node.row)
      }
    case .child:
      break
    }
    output?.push(change, pusher: self)
  }

  func reconcile(pusher: any InputBase) { output?.reconcile(pusher: self) }
}
