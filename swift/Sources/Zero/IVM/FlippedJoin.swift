/// The most entries in one batched parent fetch (`flipped-join.ts`).
nonisolated(unsafe) var multiConstraintChunkSize = 256

/// An inner join that fetches children first, then their parents
/// (`flipped-join.ts`). Outputs parents, in parent order, that have at
/// least one related child.
final class FlippedJoin: Input {
  let parent: any Input
  let child: any Input
  let parentKey: [String]
  let childKey: [String]
  let relationshipName: String
  let schema: SourceSchema
  let boundProvider: (any TakeBoundProvider)?
  /// The parents this join has output.
  let parentIndex: JoinIndex
  let indexesParents: Bool
  weak var output: (any Output)?
  private var parentOutput: ClosureOutput!
  private var childOutput: ClosureOutput!

  var inprogressChildChange: Change?
  var inprogressChildChangePosition: Row?
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
    parentIndex: JoinIndex? = nil
  ) {
    precondition(parent !== child, "Parent and child must be different operators")
    precondition(
      parentKey.count == childKey.count, "The parentKey and childKey keys must have same length")
    self.parent = parent
    self.child = child
    self.parentKey = parentKey
    self.childKey = childKey
    self.relationshipName = relationshipName
    self.boundProvider = boundProvider
    if let parentIndex {
      self.parentIndex = parentIndex
      indexesParents = false
    } else {
      self.parentIndex = JoinIndex(parentKey, parent.getSchema().primaryKey, parentPartitionKey)
      indexesParents = true
    }
    schema = parent.getSchema().adding(
      relationshipName, child.getSchema().with(isHidden: hidden, system: system))
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
    child.destroy()
    parent.destroy()
  }

  func setOutput(_ output: any Output) { self.output = output }
  func getSchema() -> SourceSchema { schema }

  /// A child node, and whether it is the in-progress removed child spliced
  /// back in (JS compares it by identity).
  typealias ChildNode = (node: Node, isRemovedOverlay: Bool)

  func fetch(_ req: FetchRequest) -> NodeStream {
    NodeStream { [self] in
      // Constraints on the parent's join key become child constraints.
      var childConstraint = Constraint()
      var hasChildConstraint = false
      if let constraint = req.constraint {
        for e in constraint.entries {
          if let index = parentKey.firstIndex(of: e.key) {
            hasChildConstraint = true
            childConstraint.set(childKey[index], e.value)
          }
        }
      }
      var childNodes: [ChildNode] = child.fetch(
        hasChildConstraint ? FetchRequest(constraint: childConstraint) : FetchRequest()
      ).map { ($0, false) }

      // Undo an in-progress remove here; it is re-applied below to the
      // parents the push has reached.
      if case .remove(let removedNode) = inprogressChildChange {
        let compare = child.getSchema().compareRows
        let insertPos = binarySearch(childNodes.count) {
          compare(removedNode.row, childNodes[$0].node.row)
        }
        childNodes.insert((removedNode, true), at: insertPos)
      }
      return fetchBatched(req, childNodes)
    }
  }

  private func fetchBatched(_ req: FetchRequest, _ childNodes: [ChildNode]) -> AnyIterator<Node> {
    let parentReqConstraint = req.constraint
    var computedMulti: [Constraint] = []
    var childIndexesByKey: [JSKey: [Int]] = [:]
    for i in 0..<childNodes.count {
      guard let fromChild = buildJoinConstraint(childNodes[i].node.row, childKey, parentKey) else {
        continue
      }
      if let parentReqConstraint, !fromChild.isCompatible(with: parentReqConstraint) { continue }
      let key = canonicalKey(fromChild, parentKey)
      if childIndexesByKey[key] == nil {
        childIndexesByKey[key] = [i]
        computedMulti.append(fromChild)
      } else {
        childIndexesByKey[key]!.append(i)
      }
    }
    if computedMulti.isEmpty { return AnyIterator { nil } }

    let compareRows = schema.compareRows
    let compare: (Node, Node) -> Int =
      req.reverse ? { compareRows($1.row, $0.row) } : { compareRows($0.row, $1.row) }
    let incoming = req.multiConstraints ?? []
    let parentStream: NodeStream
    if computedMulti.count <= multiConstraintChunkSize {
      var r = req
      r.multiConstraints = incoming + [computedMulti]
      parentStream = parent.fetch(r)
    } else {
      var chunks: [NodeStream] = []
      var i = 0
      while i < computedMulti.count {
        var r = req
        r.multiConstraints =
          incoming + [
            Array(computedMulti[i..<min(i + multiConstraintChunkSize, computedMulti.count)])
          ]
        chunks.append(parent.fetch(r))
        i += multiConstraintChunkSize
      }
      parentStream = mergeSortedStreams(chunks, compare)
    }

    var it = parentStream.makeIterator()
    return AnyIterator { [self] in
      while let node = it.next() {
        // A parent that matches none of the computed constraints comes from
        // an operator that passes multiConstraints through unfiltered.
        guard let idxs = childIndexesByKey[canonicalKey(node.row, parentKey)] else { continue }
        if let out = parentWithOverlay(node, idxs.map { childNodes[$0] }) { return out }
      }
      return nil
    }
  }

  private func parentWithOverlay(_ minParentNode: Node, _ related: [ChildNode]) -> Node? {
    var overlaid = related.map(\.node)
    // The parent has yet to get the in-progress child change if it comes
    // after the current position and a parent fetch of the push yields it.
    let compareRows = parent.getSchema().compareRows
    let isParentInPushQueue =
      inprogressChildChangePosition != nil
      && compareRows(minParentNode.row, inprogressChildChangePosition!) > 0
      && (inprogressParentFetchBounds.map {
        isInParentFetch($0, minParentNode.row, compareRows)
      } ?? true)
    if let inprogress = inprogressChildChange, inprogressChildChangePosition != nil,
      isJoinMatch(inprogress.node.row, childKey, minParentNode.row, parentKey)
    {
      if inprogress.type == .remove {
        if !isParentInPushQueue {
          // The removed child was spliced into the children above.
          overlaid = related.filter { !$0.isRemovedOverlay }.map(\.node)
        }
      } else if isParentInPushQueue {
        overlaid = Array(generateWithOverlay(NodeStream(overlaid), inprogress, child.getSchema()))
      }
    }
    if overlaid.isEmpty { return nil }
    indexParentRow(minParentNode.row)
    let children = overlaid
    return minParentNode.with(relationshipName) { NodeStream(children) }
  }

  func pushChild(_ change: Change) {
    switch change {
    case .add, .remove:
      pushChildChange(change)
    case .edit(let node, let oldNode):
      precondition(
        rowEqualsForCompoundKey(oldNode.row, node.row, childKey),
        "Child edit must not change relationship.")
      pushChildChange(change, exists: true)
    case .child:
      pushChildChange(change, exists: true)
    }
  }

  func pushChildChange(_ change: Change, exists: Bool = false) {
    inprogressChildChange = change
    inprogressChildChangePosition = nil
    defer {
      inprogressChildChange = nil
      inprogressChildChangePosition = nil
      inprogressParentFetchBounds = nil
    }
    let childRow = change.node.row
    guard let constraint = buildJoinConstraint(childRow, childKey, parentKey) else { return }
    let changeType = change.type
    let partitions = parentIndex.lookup(childRow, childKey)
    // Without a resident parent, a remove or edit cannot affect the view.
    if partitions == nil && changeType != .add && changeType != .child { return }
    let fetchConstraints: [Constraint]
    if changeType != .add && changeType != .child {
      fetchConstraints = partitions!.map { p in p.map { constraint.merging($0) } ?? constraint }
    } else {
      fetchConstraints = [constraint]
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
      inprogressChildChange = change
      inprogressChildChangePosition = parentNode.row
      let childNodeStream: () -> NodeStream = { [self] in
        guard let c = buildJoinConstraint(parentNode.row, parentKey, childKey) else {
          return .empty
        }
        return child.fetch(FetchRequest(constraint: c))
      }
      var parentExists = exists
      if !parentExists {
        let childCompare = child.getSchema().compareRows
        for childNode in childNodeStream() where childCompare(childNode.row, childRow) != 0 {
          parentExists = true
          break
        }
      }
      if parentExists {
        output?.push(
          .child(
            parentNode.with(relationshipName, childNodeStream),
            ChildChange(relationshipName: relationshipName, change: change)),
          pusher: self)
      } else {
        let changed = change.node
        let newNode = parentNode.with(relationshipName) { NodeStream([changed]) }
        if change.type == .add {
          indexParentRow(parentNode.row)
          output?.push(.add(newNode), pusher: self)
        } else {
          unindexParentRow(parentNode.row)
          output?.push(.remove(newNode), pusher: self)
        }
      }
    }
  }

  func indexParentRow(_ row: Row) {
    if indexesParents { parentIndex.add(row) }
  }

  func unindexParentRow(_ row: Row) {
    if indexesParents { parentIndex.remove(row) }
  }

  func pushParent(_ change: Change) {
    let flip: (Node) -> Node = { [self] node in
      node.with(relationshipName) { [self] in
        guard let c = buildJoinConstraint(node.row, parentKey, childKey) else { return .empty }
        return child.fetch(FetchRequest(constraint: c))
      }
    }
    // An inner join: no related child, no push.
    guard let c = buildJoinConstraint(change.node.row, parentKey, childKey),
      child.fetch(FetchRequest(constraint: c)).firstElement != nil
    else { return }
    switch change {
    case .add(let node):
      indexParentRow(node.row)
      output?.push(.add(flip(node)), pusher: self)
    case .remove(let node):
      unindexParentRow(node.row)
      output?.push(.remove(flip(node)), pusher: self)
    case .child(let node, let childChange):
      output?.push(.child(flip(node), childChange), pusher: self)
    case .edit(let node, let oldNode):
      precondition(
        rowEqualsForCompoundKey(oldNode.row, node.row, parentKey),
        "Parent edit must not change relationship.")
      unindexParentRow(oldNode.row)
      indexParentRow(node.row)
      output?.push(.edit(flip(node), oldNode: flip(oldNode)), pusher: self)
    }
  }
}

/// `binarySearch(high, compare)` (`shared/src/binary-search.ts`): the index
/// where `compare` is 0, or the first index past the elements it is
/// positive for.
func binarySearch(_ high: Int, _ compare: (Int) -> Int) -> Int {
  var low = 0
  var high = high
  while low < high {
    let mid = low + ((high - low) >> 1)
    let i = compare(mid)
    if i == 0 { return mid }
    if i > 0 { low = mid + 1 } else { high = mid }
  }
  return low
}
