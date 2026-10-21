/// Connects a pipeline being built to its environment
/// (`BuilderDelegate` in `packages/zql/src/builder/builder.ts`).
package protocol BuilderDelegate: AnyObject {
  /// Apply `where` filters even when the source applies them all.
  var applyFiltersAnyway: Bool { get }
  /// Allow NOT EXISTS. Only the server does: a client cannot tell a row
  /// that does not exist from one that is not synced.
  var enableNotExists: Bool { get }
  /// Do not copy a parent's conditions on correlation columns into its
  /// subqueries.
  var disableCorrelatedPredicatePushdown: Bool { get }
  func getSource(_ tableName: String) -> (any Source)?
  func decorateInput(_ input: any Input, name: String) -> any Input
  func addEdge(_ source: any InputBase, _ dest: any InputBase)
  func decorateFilterInput(_ input: any FilterInput, name: String) -> any FilterInput
  func decorateSourceInput(_ input: any SourceInput, queryID: String) -> any Input
  func mapAst(_ ast: AST) -> AST
}

extension BuilderDelegate {
  package var applyFiltersAnyway: Bool { false }
  package var enableNotExists: Bool { false }
  package var disableCorrelatedPredicatePushdown: Bool { false }
  package func decorateInput(_ input: any Input, name: String) -> any Input { input }
  package func addEdge(_ source: any InputBase, _ dest: any InputBase) {}
  package func decorateFilterInput(_ input: any FilterInput, name: String) -> any FilterInput {
    input
  }
  package func decorateSourceInput(_ input: any SourceInput, queryID: String) -> any Input { input }
  package func mapAst(_ ast: AST) -> AST { ast }
}

/// The limit put on EXISTS subqueries: one row is enough, a few more keep
/// a removal from emptying it.
let existsLimit = 3
let permissionsExistsLimit = 1

/// Builds a pipeline from an AST (`buildPipeline`).
package func buildPipeline(
  _ ast: AST, _ delegate: any BuilderDelegate, queryID: String
)
  throws(PipelineError) -> any Input
{
  var ast = delegate.mapAst(ast)
  ast = try completeOrdering(ast) { (table) throws(PipelineError) in
    guard let source = delegate.getSource(table) else {
      throw PipelineError("Source not found: \(table)")
    }
    return source.tableSchema.primaryKey
  }
  if !delegate.disableCorrelatedPredicatePushdown {
    ast = pushDownCorrelatedPredicates(ast) { delegate.getSource($0)?.tableSchema.columns ?? [:] }
  }
  return try buildPipelineInternal(ast, delegate, queryID, "")
}

/// Values for static parameters (`StaticQueryParameters`).
package struct StaticQueryParameters {
  package var authData: [String: JSONValue]?
  package var preMutationRow: Row?

  package init(authData: [String: JSONValue]?, preMutationRow: Row? = nil) {
    self.authData = authData
    self.preMutationRow = preMutationRow
  }
}

/// `bindStaticParameters`: replaces static parameters with their values.
package func bindStaticParameters(_ ast: AST, _ params: StaticQueryParameters?) -> AST {
  func bindValue(_ v: AST.ValuePosition) -> AST.ValuePosition {
    guard case .static(let anchor, let field) = v else { return v }
    guard let params else { preconditionFailure("Static query params do not exist") }
    let anchorValue: [String: JSONValue]? =
      anchor == .authData ? params.authData : params.preMutationRow?.dictionary
    guard let anchorValue else { return .literal(.null) }
    switch field {
    case .single(let f):
      return .literal(anchorValue[f] ?? .null)
    case .path(let path):
      var current: JSONValue? = .object(anchorValue)
      for f in path { current = current?.objectValue?[f] }
      return .literal(current ?? .null)
    }
  }
  func bindCondition(_ c: AST.Condition) -> AST.Condition {
    switch c {
    case .simple(let op, let left, let right):
      return .simple(op: op, left: bindValue(left), right: bindValue(right))
    case .correlatedSubquery(var related, let op, let flip, let scalar):
      related.subquery = visit(related.subquery)
      return .correlatedSubquery(related: related, op: op, flip: flip, scalar: scalar)
    case .and(let cs): return .and(cs.map(bindCondition))
    case .or(let cs): return .or(cs.map(bindCondition))
    }
  }
  func visit(_ node: AST) -> AST {
    var n = node
    n.where = node.where.map(bindCondition)
    n.related = node.related?.map { sq in
      var s = sq
      s.subquery = visit(sq.subquery)
      return s
    }
    return n
  }
  return visit(ast)
}

/// Throws if `condition` has a NOT EXISTS outside nested subqueries.
func assertNoNotExists(_ condition: AST.Condition) throws(PipelineError) {
  switch condition {
  case .simple: return
  case .correlatedSubquery(_, let op, _, _):
    if op == .notExists {
      throw PipelineError(
        "not(exists()) is not supported on the client - see https://bugs.rocicorp.dev/issue/3438")
    }
  case .and(let cs), .or(let cs):
    for c in cs { try assertNoNotExists(c) }
  }
}

struct CSQCondition {
  var related: AST.CorrelatedSubquery
  var op: AST.CorrelatedSubqueryOperator
  var flip: Bool?
}

func buildPipelineInternal(
  _ astIn: AST,
  _ delegate: any BuilderDelegate,
  _ queryID: String,
  _ name: String,
  partitionKey: [String]? = nil,
  isNonFlippedExistsChild: Bool = false
) throws(PipelineError) -> any Input {
  guard let source = delegate.getSource(astIn.table) else {
    throw PipelineError("Source not found: \(astIn.table)")
  }
  let ast = uniquifyCorrelatedSubqueryConditionAliases(astIn)
  if !delegate.enableNotExists, let w = ast.where { try assertNoNotExists(w) }

  let csqConditions = gatherCorrelatedSubqueryQueryConditions(ast.where)
  var splitEditKeys = Set(partitionKey ?? [])
  for csq in csqConditions {
    for key in csq.related.correlation.parentField { splitEditKeys.insert(key) }
  }
  for csq in ast.related ?? [] {
    for key in csq.correlation.parentField { splitEditKeys.insert(key) }
  }
  if isNonFlippedExistsChild {
    precondition(ast.start == nil, "EXISTS subqueries must not have start")
    precondition(ast.related == nil, "EXISTS subqueries must not have related")
  }

  // Cap needs an unordered connection, but a flipped subquery in the where
  // builds a UnionFanIn, which needs order. Fall back to Take then.
  let useCap =
    isNonFlippedExistsChild && !(ast.where.map(conditionIncludesFlippedSubqueryAtAnyLevel) ?? false)

  let conn = try source.connect(
    sort: useCap ? nil : ast.orderBy!, filters: ast.where, splitEditKeys: splitEditKeys)
  var end: any Input = delegate.decorateSourceInput(conn, queryID: queryID)
  end = delegate.decorateInput(end, name: "\(name):source(\(ast.table))")
  let fullyAppliedFilters = conn.fullyAppliedFilters

  if let start = ast.start {
    let skip = Skip(end, start)
    delegate.addEdge(end, skip)
    end = delegate.decorateInput(skip, name: "\(name):skip)")
  }

  var takeGate: TakeGate?
  if ast.limit != nil && !useCap {
    let gate = TakeGate(end)
    delegate.addEdge(end, gate)
    end = delegate.decorateInput(gate, name: "\(name):take-gate")
    takeGate = gate
  }

  // The parent indexes of the EXISTS joins, kept by a JoinIndexTap after
  // the limit.
  var existsParentIndexes: [JoinIndex] = []
  for csq in csqConditions where csq.flip != true {
    let parentIndex = JoinIndex(csq.related.correlation.parentField, end.getSchema().primaryKey)
    existsParentIndexes.append(parentIndex)
    var related = csq.related
    related.subquery.limit = Double(
      related.system == .permissions ? permissionsExistsLimit : existsLimit)
    end = try applyCorrelatedSubQuery(
      related, delegate, queryID, end, name, fromCondition: true, parentPartitionKey: nil,
      boundProvider: takeGate, exists: ExistsJoin(op: csq.op, parentIndex: parentIndex))
  }

  if let w = ast.where, !fullyAppliedFilters || delegate.applyFiltersAnyway {
    end = try applyWhere(
      end, w, delegate, name, partitionKey, takeGate, &existsParentIndexes, trackIndexes: true)
  }

  if let limitValue = ast.limit {
    let limit = Int(limitValue)
    if useCap {
      let cap = Cap(end, limit: limit, partitionKey: partitionKey)
      delegate.addEdge(end, cap)
      end = delegate.decorateInput(cap, name: "\(name):cap")
    } else {
      let take = try Take(end, limit: limit, partitionKey: partitionKey)
      delegate.addEdge(end, take)
      end = delegate.decorateInput(take, name: "\(name):take")
      if let takeGate {
        takeGate.setBoundProvider(take)
        take.setTakeGate(takeGate)
      }
    }
  }

  if !existsParentIndexes.isEmpty {
    let tap = JoinIndexTap(end, existsParentIndexes)
    delegate.addEdge(end, tap)
    end = delegate.decorateInput(tap, name: "\(name):join-index")
  }

  if let related = ast.related {
    // Dedupe by alias, last one wins, keeping the first one's position.
    var order: [String] = []
    var byAlias: [String: AST.CorrelatedSubquery] = [:]
    for csq in related {
      let alias = csq.subquery.alias ?? ""
      if byAlias[alias] == nil { order.append(alias) }
      byAlias[alias] = csq
    }
    for alias in order {
      end = try applyCorrelatedSubQuery(
        byAlias[alias]!, delegate, queryID, end, name, fromCondition: false,
        parentPartitionKey: partitionKey)
    }
  }
  return end
}

func applyWhere(
  _ input: any Input,
  _ condition: AST.Condition,
  _ delegate: any BuilderDelegate,
  _ name: String,
  _ parentPartitionKey: [String]?,
  _ boundProvider: (any TakeBoundProvider)?,
  _ existsParentIndexes: inout [JoinIndex],
  trackIndexes: Bool
) throws(PipelineError) -> any Input {
  if !conditionIncludesFlippedSubqueryAtAnyLevel(condition) {
    return try buildFilterPipeline(
      input, delegate,
      { (filterInput) throws(PipelineError) in
        try applyFilter(filterInput, condition, delegate, name)
      },
      transformFilters(condition).filters)
  }
  return try applyFilterWithFlips(
    input, condition, delegate, name, parentPartitionKey, boundProvider, &existsParentIndexes,
    trackIndexes: trackIndexes)
}

func applyFilterWithFlips(
  _ input: any Input,
  _ condition: AST.Condition,
  _ delegate: any BuilderDelegate,
  _ name: String,
  _ parentPartitionKey: [String]?,
  _ boundProvider: (any TakeBoundProvider)?,
  _ existsParentIndexes: inout [JoinIndex],
  trackIndexes: Bool
) throws(PipelineError) -> any Input {
  var end = input
  switch condition {
  case .simple:
    preconditionFailure("Simple conditions cannot have flips")
  case .and(let conditions):
    let (withFlipped, withoutFlipped) = partitionBranches(
      conditions, conditionIncludesFlippedSubqueryAtAnyLevel)
    if !withoutFlipped.isEmpty {
      let andCondition = AST.Condition.and(withoutFlipped)
      end = try buildFilterPipeline(
        input, delegate,
        { (filterInput) throws(PipelineError) in
          try applyAnd(filterInput, withoutFlipped, delegate, name)
        },
        transformFilters(andCondition).filters)
    }
    precondition(!withFlipped.isEmpty, "Impossible to have no flips here")
    for cond in withFlipped {
      end = try applyFilterWithFlips(
        end, cond, delegate, name, parentPartitionKey, boundProvider, &existsParentIndexes,
        trackIndexes: trackIndexes)
    }
  case .or(let conditions):
    let (withFlipped, withoutFlipped) = partitionBranches(
      conditions, conditionIncludesFlippedSubqueryAtAnyLevel)
    precondition(!withFlipped.isEmpty, "Impossible to have no flips here")
    let ufo = UnionFanOut(end)
    delegate.addEdge(end, ufo)
    end = delegate.decorateInput(ufo, name: "\(name):ufo")
    var branches: [any Input] = []
    if !withoutFlipped.isEmpty {
      let orCondition = AST.Condition.or(withoutFlipped)
      branches.append(
        try buildFilterPipeline(
          end, delegate,
          { (filterInput) throws(PipelineError) in
            try applyOr(filterInput, withoutFlipped, delegate, name)
          },
          transformFilters(orCondition).filters))
    }
    for cond in withFlipped {
      branches.append(
        try applyFilterWithFlips(
          end, cond, delegate, name, parentPartitionKey, boundProvider, &existsParentIndexes,
          trackIndexes: trackIndexes))
    }
    let ufi = UnionFanIn(ufo, branches)
    for branch in branches { delegate.addEdge(branch, ufi) }
    end = delegate.decorateInput(ufi, name: "\(name):ufi")
  case .correlatedSubquery(let sq, _, _, _):
    let child = try buildPipelineInternal(
      sq.subquery, delegate, "", "\(name).\(sq.subquery.alias ?? "")",
      partitionKey: sq.correlation.childField, isNonFlippedExistsChild: false)
    var parentIndex: JoinIndex?
    if trackIndexes {
      let index = JoinIndex(
        sq.correlation.parentField, end.getSchema().primaryKey, parentPartitionKey)
      existsParentIndexes.append(index)
      parentIndex = index
    }
    guard let alias = sq.subquery.alias else { preconditionFailure("Subquery must have an alias") }
    let flippedJoin = FlippedJoin(
      parent: end, child: child, parentKey: sq.correlation.parentField,
      childKey: sq.correlation.childField, relationshipName: alias, hidden: sq.hidden ?? false,
      system: sq.system ?? .client, parentPartitionKey: parentPartitionKey,
      boundProvider: boundProvider, parentIndex: parentIndex)
    delegate.addEdge(end, flippedJoin)
    delegate.addEdge(child, flippedJoin)
    end = delegate.decorateInput(flippedJoin, name: "\(name):flipped-join(\(alias))")
  }
  return end
}

func applyFilter(
  _ input: any FilterInput, _ condition: AST.Condition, _ delegate: any BuilderDelegate,
  _ name: String
) throws(PipelineError) -> any FilterInput {
  switch condition {
  case .and(let cs): return try applyAnd(input, cs, delegate, name)
  case .or(let cs): return try applyOr(input, cs, delegate, name)
  case .correlatedSubquery(let related, let op, _, _):
    return applyCorrelatedSubqueryCondition(input, related, op, delegate, name)
  case .simple:
    return try applySimpleCondition(input, delegate, condition)
  }
}

func applyAnd(
  _ input: any FilterInput, _ conditions: [AST.Condition], _ delegate: any BuilderDelegate,
  _ name: String
) throws(PipelineError) -> any FilterInput {
  var input = input
  for c in conditions { input = try applyFilter(input, c, delegate, name) }
  return input
}

func applyOr(
  _ input: any FilterInput, _ conditions: [AST.Condition], _ delegate: any BuilderDelegate,
  _ name: String
) throws(PipelineError) -> any FilterInput {
  let (subqueryConditions, otherConditions) = groupSubqueryConditions(conditions)
  // Without subqueries no fan-out is needed.
  if subqueryConditions.isEmpty {
    let filter = Filter(input, try createPredicate(.or(otherConditions)))
    delegate.addEdge(input, filter)
    return filter
  }
  let fanOut = FanOut(input)
  delegate.addEdge(input, fanOut)
  var branches: [any FilterInput] = []
  for c in subqueryConditions { branches.append(try applyFilter(fanOut, c, delegate, name)) }
  if !otherConditions.isEmpty {
    let filter = Filter(fanOut, try createPredicate(.or(otherConditions)))
    delegate.addEdge(fanOut, filter)
    branches.append(filter)
  }
  let ret = FanIn(fanOut, branches)
  for branch in branches { delegate.addEdge(branch, ret) }
  fanOut.setFanIn(ret)
  return ret
}

func groupSubqueryConditions(
  _ conditions: [AST.Condition]
) -> (
  subquery: [AST.Condition], other: [AST.Condition]
) {
  var subquery: [AST.Condition] = []
  var other: [AST.Condition] = []
  for c in conditions {
    if isNotAndDoesNotContainSubquery(c) { other.append(c) } else { subquery.append(c) }
  }
  return (subquery, other)
}

func isNotAndDoesNotContainSubquery(_ condition: AST.Condition) -> Bool {
  switch condition {
  case .correlatedSubquery: false
  case .simple: true
  case .and(let cs), .or(let cs): cs.allSatisfy(isNotAndDoesNotContainSubquery)
  }
}

func applySimpleCondition(
  _ input: any FilterInput, _ delegate: any BuilderDelegate, _ condition: AST.Condition
) throws(PipelineError) -> any FilterInput {
  let filter = Filter(input, try createPredicate(condition))
  if case .simple(let op, let left, let right) = condition {
    _ = delegate.decorateFilterInput(
      filter, name: "\(valuePosName(left)):\(op.rawValue):\(valuePosName(right))")
  }
  delegate.addEdge(input, filter)
  return filter
}

func valuePosName(_ v: AST.ValuePosition) -> String {
  switch v {
  case .static(_, .single(let f)): f
  case .static(_, .path(let p)): p.joined(separator: ",")
  case .literal(let value): JSString.from(value)
  case .column(let name): name
  }
}

func applyCorrelatedSubQuery(
  _ sq: AST.CorrelatedSubquery,
  _ delegate: any BuilderDelegate,
  _ queryID: String,
  _ end: any Input,
  _ name: String,
  fromCondition: Bool,
  parentPartitionKey: [String]?,
  boundProvider: (any TakeBoundProvider)? = nil,
  exists: ExistsJoin? = nil
) throws(PipelineError) -> any Input {
  // Only omitted for conditions: `related` with `limit(0)` still gets an
  // empty array.
  if sq.subquery.limit == 0 && fromCondition { return end }
  guard let alias = sq.subquery.alias else { preconditionFailure("Subquery must have an alias") }
  let child = try buildPipelineInternal(
    sq.subquery, delegate, queryID, "\(name).\(alias)", partitionKey: sq.correlation.childField,
    isNonFlippedExistsChild: fromCondition)
  let join = Join(
    parent: end, child: child, parentKey: sq.correlation.parentField,
    childKey: sq.correlation.childField, relationshipName: alias, hidden: sq.hidden ?? false,
    system: sq.system ?? .client, parentPartitionKey: fromCondition ? nil : parentPartitionKey,
    boundProvider: boundProvider, exists: exists)
  delegate.addEdge(end, join)
  delegate.addEdge(child, join)
  return delegate.decorateInput(join, name: "\(name):join(\(alias))")
}

func applyCorrelatedSubqueryCondition(
  _ input: any FilterInput,
  _ related: AST.CorrelatedSubquery,
  _ op: AST.CorrelatedSubqueryOperator,
  _ delegate: any BuilderDelegate,
  _ name: String
) -> any FilterInput {
  if related.subquery.limit == 0 {
    let result = op != .exists
    let filter = Filter(input) { _ in result }
    delegate.addEdge(input, filter)
    return filter
  }
  let alias = related.subquery.alias!
  let exists = Exists(input, alias, related.correlation.parentField, op)
  delegate.addEdge(input, exists)
  return delegate.decorateFilterInput(exists, name: "\(name):exists(\(alias))")
}

func gatherCorrelatedSubqueryQueryConditions(_ condition: AST.Condition?) -> [CSQCondition] {
  var csqs: [CSQCondition] = []
  func gather(_ c: AST.Condition) {
    switch c {
    case .correlatedSubquery(let related, let op, let flip, _):
      csqs.append(CSQCondition(related: related, op: op, flip: flip))
    case .and(let cs), .or(let cs):
      for x in cs { gather(x) }
    case .simple:
      break
    }
  }
  if let condition { gather(condition) }
  return csqs
}

/// Suffixes the aliases of a where's subqueries with a counter so that
/// each gets its own relationship.
func uniquifyCorrelatedSubqueryConditionAliases(_ ast: AST) -> AST {
  guard let w = ast.where else { return ast }
  switch w {
  case .and, .or: break
  default: return ast
  }
  var count = 0
  func uniquify(_ c: AST.Condition) -> AST.Condition {
    switch c {
    case .simple: return c
    case .correlatedSubquery(var related, let op, let flip, let scalar):
      related.subquery.alias = (related.subquery.alias ?? "") + "_" + String(count)
      count += 1
      return .correlatedSubquery(related: related, op: op, flip: flip, scalar: scalar)
    case .and(let cs): return .and(cs.map(uniquify))
    case .or(let cs): return .or(cs.map(uniquify))
    }
  }
  var result = ast
  result.where = uniquify(w)
  return result
}

func conditionIncludesFlippedSubqueryAtAnyLevel(_ cond: AST.Condition) -> Bool {
  switch cond {
  case .correlatedSubquery(_, _, let flip, _): flip ?? false
  case .and(let cs), .or(let cs): cs.contains(where: conditionIncludesFlippedSubqueryAtAnyLevel)
  case .simple: false
  }
}

func partitionBranches(
  _ conditions: [AST.Condition], _ predicate: (AST.Condition) -> Bool
) -> (
  [AST.Condition], [AST.Condition]
) {
  var matched: [AST.Condition] = []
  var notMatched: [AST.Condition] = []
  for c in conditions {
    if predicate(c) { matched.append(c) } else { notMatched.append(c) }
  }
  return (matched, notMatched)
}

/// `completeOrdering`: appends missing primary key columns to every
/// ordering in the AST.
func completeOrdering(
  _ ast: AST, _ getPrimaryKey: (String) throws(PipelineError) -> [String]
) throws(PipelineError) -> AST {
  let primaryKey = try getPrimaryKey(ast.table)
  var result = ast
  if let related = ast.related {
    var out: [AST.CorrelatedSubquery] = []
    for r in related {
      var c = r
      c.subquery = try completeOrdering(r.subquery, getPrimaryKey)
      out.append(c)
    }
    result.related = out
  }
  if let w = ast.where {
    result.where = try completeOrderingInCondition(w, getPrimaryKey)
  }
  result.orderBy = addPrimaryKeys(primaryKey, ast.orderBy)
  return result
}

private func completeOrderingInCondition(
  _ c: AST.Condition, _ getPrimaryKey: (String) throws(PipelineError) -> [String]
) throws(PipelineError) -> AST.Condition {
  switch c {
  case .simple: return c
  case .correlatedSubquery(var related, let op, let flip, let scalar):
    related.subquery = try completeOrdering(related.subquery, getPrimaryKey)
    return .correlatedSubquery(related: related, op: op, flip: flip, scalar: scalar)
  case .and(let cs):
    var out: [AST.Condition] = []
    for x in cs { out.append(try completeOrderingInCondition(x, getPrimaryKey)) }
    return .and(out)
  case .or(let cs):
    var out: [AST.Condition] = []
    for x in cs { out.append(try completeOrderingInCondition(x, getPrimaryKey)) }
    return .or(out)
  }
}

func addPrimaryKeys(_ primaryKey: [String], _ orderBy: AST.Ordering?) -> AST.Ordering {
  let orderBy = orderBy ?? []
  var toAdd = primaryKey
  var seen = Set<String>()
  toAdd = toAdd.filter { seen.insert($0).inserted }
  for part in orderBy { toAdd.removeAll { $0 == part.field } }
  if toAdd.isEmpty { return orderBy }
  return orderBy + toAdd.map { AST.OrderPart($0, .asc) }
}
