/// `filterPush` (`packages/zql/src/ivm/filter-push.ts`).
func filterPush(
  _ change: Change, _ output: (any Output)?, _ pusher: any InputBase, _ predicate: RowPredicate?
) {
  guard let predicate else {
    output?.push(change, pusher: pusher)
    return
  }
  switch change {
  case .add(let node), .remove(let node), .child(let node, _):
    if predicate(node.row) { output?.push(change, pusher: pusher) }
  case .edit(let node, let oldNode):
    maybeSplitAndPushEditChange(node, oldNode, predicate, output, pusher)
  }
}

/// `maybeSplitAndPushEditChange`: an edit whose row enters or leaves the
/// predicate becomes an add or a remove.
func maybeSplitAndPushEditChange(
  _ node: Node, _ oldNode: Node, _ predicate: RowPredicate, _ output: (any Output)?,
  _ pusher: any InputBase
) {
  let oldWasPresent = predicate(oldNode.row)
  let newIsPresent = predicate(node.row)
  if oldWasPresent && newIsPresent {
    output?.push(.edit(node, oldNode: oldNode), pusher: pusher)
  } else if oldWasPresent {
    output?.push(.remove(oldNode), pusher: pusher)
  } else if newIsPresent {
    output?.push(.add(node), pusher: pusher)
  }
}

/// Filters through a pure predicate (`packages/zql/src/ivm/filter.ts`).
final class Filter: FilterOperator {
  let input: any FilterInput
  let predicate: RowPredicate
  weak var output: (any FilterOutput)?

  init(_ input: any FilterInput, _ predicate: @escaping RowPredicate) {
    self.input = input
    self.predicate = predicate
    input.setFilterOutput(self)
  }

  func beginFilter() { output?.beginFilter() }
  func endFilter() { output?.endFilter() }

  func filter(_ node: Node) -> Bool {
    predicate(node.row) ? (output?.filter(node) ?? false) : false
  }

  func setFilterOutput(_ output: any FilterOutput) { self.output = output }
  func destroy() { input.destroy() }
  func getSchema() -> SourceSchema { input.getSchema() }

  func push(_ change: Change, pusher: any InputBase) {
    filterPush(change, output, self, predicate)
  }

  func reconcile(pusher: any InputBase) { output?.reconcile(pusher: self) }
}

/// Adapts an `Input` to the `where` sub-graph (`filter-operators.ts`).
final class FilterStart: FilterInput, Output {
  let input: any Input
  let condition: AST.Condition?
  weak var output: (any FilterOutput)?

  init(_ input: any Input, _ condition: AST.Condition?) {
    self.input = input
    self.condition = condition
    input.setOutput(self)
  }

  func setFilterOutput(_ output: any FilterOutput) { self.output = output }
  func destroy() { input.destroy() }
  func getSchema() -> SourceSchema { input.getSchema() }

  func push(_ change: Change, pusher: any InputBase) { output?.push(change, pusher: self) }
  func reconcile(pusher: any InputBase) { output?.reconcile(pusher: self) }

  func fetch(_ req: FetchRequest) -> NodeStream {
    NodeStream { [self] in
      var childReq = req
      childReq.filter = mergeFilters(req.filter, condition)
      return AnyIterator(FilterIterator(input.fetch(childReq).makeIterator(), output))
    }
  }
}

/// `FilterStart.fetch`'s generator: `beginFilter`, then `endFilter` once the
/// stream ends or is abandoned (the JS `finally`).
final class FilterIterator: IteratorProtocol {
  var input: LazyIterator<Node>?
  weak var output: (any FilterOutput)?

  init(_ input: LazyIterator<Node>, _ output: (any FilterOutput)?) {
    self.input = input
    self.output = output
    output?.beginFilter()
  }

  func next() -> Node? {
    while let node = input?.next() {
      if output?.filter(node) ?? false { return node }
    }
    finish()
    return nil
  }

  func finish() {
    if input != nil {
      input = nil
      output?.endFilter()
    }
  }

  deinit { finish() }
}

func mergeFilters(_ reqFilter: AST.Condition?, _ own: AST.Condition?) -> AST.Condition? {
  guard let own else { return reqFilter }
  guard let reqFilter else { return own }
  return .and([reqFilter, own])
}

/// Adapts the end of the `where` sub-graph back to an `Input`.
final class FilterEnd: Input, FilterOutput {
  let start: FilterStart
  let input: any FilterInput
  weak var output: (any Output)?

  init(_ start: FilterStart, _ input: any FilterInput) {
    self.start = start
    self.input = input
    input.setFilterOutput(self)
  }

  func fetch(_ req: FetchRequest) -> NodeStream { start.fetch(req) }
  func beginFilter() {}
  func endFilter() {}
  func filter(_ node: Node) -> Bool { true }
  func setOutput(_ output: any Output) { self.output = output }
  func destroy() { input.destroy() }
  func getSchema() -> SourceSchema { input.getSchema() }
  func push(_ change: Change, pusher: any InputBase) { output?.push(change, pusher: self) }
  func reconcile(pusher: any InputBase) { output?.reconcile(pusher: self) }
}

/// Filters on whether a relationship is non-empty (`exists.ts`).
final class Exists: FilterOperator {
  let input: any FilterInput
  let relationshipName: String
  let not: Bool
  let parentJoinKey: [String]
  let noSizeReuse: Bool
  var cache: [String: Bool] = [:]
  weak var output: (any FilterOutput)?
  /// Set while processing a push, when relationships can be inconsistent,
  /// to stop reusing cached sizes across rows with the same join key.
  var inPush = false

  init(
    _ input: any FilterInput, _ relationshipName: String, _ parentJoinKey: [String],
    _ type: AST.CorrelatedSubqueryOperator
  ) {
    self.input = input
    self.relationshipName = relationshipName
    self.not = type == .notExists
    self.parentJoinKey = parentJoinKey
    let schema = input.getSchema()
    precondition(
      schema.relationship(relationshipName) != nil, "Input schema missing \(relationshipName)")
    noSizeReuse = parentJoinKey == schema.primaryKey
    input.setFilterOutput(self)
  }

  func setFilterOutput(_ output: any FilterOutput) { self.output = output }

  func beginFilter() { output?.beginFilter() }

  func endFilter() {
    cache = [:]
    output?.endFilter()
  }

  func filter(_ node: Node) -> Bool {
    var exists: Bool?
    if !noSizeReuse && !inPush {
      let key = cacheKey(node)
      exists = cache[key]
      if exists == nil {
        exists = fetchSize(node) > 0
        cache[key] = exists
      }
    }
    return passes(node, exists) && (output?.filter(node) ?? false)
  }

  func destroy() { input.destroy() }
  func getSchema() -> SourceSchema { input.getSchema() }
  func reconcile(pusher: any InputBase) { output?.reconcile(pusher: self) }

  func push(_ change: Change, pusher: any InputBase) {
    precondition(!inPush, "Unexpected re-entrancy")
    inPush = true
    defer { inPush = false }
    switch change {
    case .add, .edit, .remove:
      pushWithFilter(change)
    case .child(let node, let child):
      if child.relationshipName != relationshipName || child.change.type == .edit
        || child.change.type == .child
      {
        pushWithFilter(change)
        return
      }
      switch child.change {
      case .add:
        let size = fetchSize(node)
        if size == 1 {
          if not {
            output?.push(.remove(node.with(relationshipName) { .empty }), pusher: self)
          } else {
            output?.push(.add(node), pusher: self)
          }
        } else {
          pushWithFilter(change, size > 0)
        }
      case .remove(let removed):
        let size = fetchSize(node)
        if size == 0 {
          if not {
            output?.push(.add(node), pusher: self)
          } else {
            output?.push(
              .remove(node.with(relationshipName) { NodeStream([removed]) }), pusher: self)
          }
        } else {
          pushWithFilter(change, size > 0)
        }
      default:
        break
      }
    }
  }

  func passes(_ node: Node, _ exists: Bool? = nil) -> Bool {
    let e = exists ?? (fetchSize(node) > 0)
    return not ? !e : e
  }

  func cacheKey(_ node: Node) -> String {
    JSONSerializer.stringify(.array(parentJoinKey.map { node.row[$0] ?? .null }))
  }

  func pushWithFilter(_ change: Change, _ exists: Bool? = nil) {
    if passes(change.node, exists) { output?.push(change, pusher: self) }
  }

  func fetchSize(_ node: Node) -> Int {
    guard let relationship = node.relationships[relationshipName] else {
      preconditionFailure("Exists: relationship \"\(relationshipName)\" not found on node")
    }
    var size = 0
    for _ in relationship() { size += 1 }
    return size
  }
}

/// Forks the `where` sub-graph for an OR (`fan-out.ts`).
final class FanOut: FilterOperator {
  let input: any FilterInput
  var outputs: [WeakFilterOutput] = []
  weak var fanIn: FanIn?
  var destroyCount = 0

  init(_ input: any FilterInput) {
    self.input = input
    input.setFilterOutput(self)
  }

  func setFanIn(_ fanIn: FanIn) { self.fanIn = fanIn }
  func setFilterOutput(_ output: any FilterOutput) { outputs.append(WeakFilterOutput(output)) }

  func destroy() {
    precondition(destroyCount < outputs.count, "FanOut already destroyed once for each output")
    destroyCount += 1
    if destroyCount == outputs.count { input.destroy() }
  }

  func getSchema() -> SourceSchema { input.getSchema() }
  func beginFilter() { for o in outputs { o.value?.beginFilter() } }
  func endFilter() { for o in outputs { o.value?.endFilter() } }

  func filter(_ node: Node) -> Bool {
    for o in outputs where o.value?.filter(node) ?? false { return true }
    return false
  }

  func push(_ change: Change, pusher: any InputBase) {
    for o in outputs { o.value?.push(change, pusher: self) }
    guard let fanIn else { preconditionFailure("fan-out must have a corresponding fan-in set!") }
    fanIn.fanOutDonePushingToAllBranches(change.type)
  }

  func reconcile(pusher: any InputBase) {
    for o in outputs { o.value?.reconcile(pusher: self) }
    fanIn?.fanOutDoneReconciling()
  }
}

/// Merges the branches of an OR, removing duplicates (`fan-in.ts`).
final class FanIn: FilterOperator {
  let inputs: [any FilterInput]
  let schema: SourceSchema
  weak var output: (any FilterOutput)?
  var accumulatedPushes: [Change] = []

  init(_ fanOut: FanOut, _ inputs: [any FilterInput]) {
    self.inputs = inputs
    schema = fanOut.getSchema()
    for input in inputs {
      input.setFilterOutput(self)
      precondition(schema === input.getSchema(), "Schema mismatch in fan-in")
    }
  }

  func setFilterOutput(_ output: any FilterOutput) { self.output = output }
  func destroy() { for i in inputs { i.destroy() } }
  func getSchema() -> SourceSchema { schema }
  func beginFilter() { output?.beginFilter() }
  func endFilter() { output?.endFilter() }
  func filter(_ node: Node) -> Bool { output?.filter(node) ?? false }

  func push(_ change: Change, pusher: any InputBase) {
    accumulatedPushes.append(change)
  }

  func fanOutDonePushingToAllBranches(_ fanOutChangeType: ChangeType) {
    if inputs.isEmpty {
      precondition(
        accumulatedPushes.isEmpty,
        "If there are no inputs then fan-in should not receive any pushes.")
      return
    }
    guard let output else {
      accumulatedPushes.removeAll()
      return
    }
    pushAccumulatedChanges(
      &accumulatedPushes, output, self, fanOutChangeType, mergeRelationships: { a, _ in a },
      addEmptyRelationships: { $0 })
  }

  func reconcile(pusher: any InputBase) {}

  func fanOutDoneReconciling() {
    if inputs.isEmpty { return }
    output?.reconcile(pusher: self)
  }
}

/// `buildFilterPipeline`: wraps the `where` sub-graph `pipeline` builds
/// between a `FilterStart` and a `FilterEnd`.
func buildFilterPipeline(
  _ input: any Input,
  _ delegate: any BuilderDelegate,
  _ pipeline: (any FilterInput) throws(PipelineError) -> any FilterInput,
  _ condition: AST.Condition?
) throws(PipelineError) -> any Input {
  let filterStart = FilterStart(input, condition)
  delegate.addEdge(input, filterStart)
  let middle = try pipeline(filterStart)
  delegate.addEdge(filterStart, middle)
  let filterEnd = FilterEnd(filterStart, middle)
  delegate.addEdge(middle, filterEnd)
  return filterEnd
}

/// A weak reference to a filter output, for operators with several.
struct WeakFilterOutput {
  weak var value: (any FilterOutput)?
  init(_ value: any FilterOutput) { self.value = value }
}

/// A weak reference to an output, for operators with several.
struct WeakOutput {
  weak var value: (any Output)?
  init(_ value: any Output) { self.value = value }
}
