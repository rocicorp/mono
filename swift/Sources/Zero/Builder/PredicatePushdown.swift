/// The longest `IN` list copied into a child.
let maxPushedInValues = 32

/// Copies a parent's conditions on correlation columns into its subqueries
/// (`correlated-predicate-pushdown.ts`). The result is the same; child rows
/// that cannot join to a parent that matters are filtered out early.
func pushDownCorrelatedPredicates(
  _ ast: AST, _ columnsOf: @escaping (String) -> [String: ColumnSchema]
) -> AST {
  func visit(_ ast: AST) -> AST {
    let w = ast.where.map { visitCondition($0, ast.table, []) }
    let facts = w.map(simpleConjuncts) ?? []
    var result = ast
    result.where = w
    result.related = ast.related?.map { csq in
      var c = csq
      c.subquery = visit(pushInto(csq, facts, ast.table))
      return c
    }
    return result
  }

  func visitCondition(
    _ c: AST.Condition, _ table: String, _ facts: [AST.Condition]
  )
    -> AST.Condition
  {
    switch c {
    case .simple:
      return c
    case .and:
      return visitConjunction(c, table, facts + simpleConjuncts(c))
    case .or(let cs):
      return .or(cs.map { visitCondition($0, table, facts) })
    case .correlatedSubquery(var related, let op, let flip, let scalar):
      related.subquery = visit(pushInto(related, facts, table))
      return .correlatedSubquery(related: related, op: op, flip: flip, scalar: scalar)
    }
  }

  // `facts` already has the simple conjuncts of `c` and its nested ANDs.
  func visitConjunction(
    _ c: AST.Condition, _ table: String, _ facts: [AST.Condition]
  )
    -> AST.Condition
  {
    guard case .and(let cs) = c else { return c }
    return .and(
      cs.map { x in
        if case .and = x { return visitConjunction(x, table, facts) }
        return visitCondition(x, table, facts)
      })
  }

  func pushInto(
    _ csq: AST.CorrelatedSubquery, _ facts: [AST.Condition], _ parentTable: String
  )
    -> AST
  {
    let parentField = csq.correlation.parentField
    let childField = csq.correlation.childField
    let subquery = csq.subquery
    var conjuncts = subquery.where.map(simpleConjuncts) ?? []
    var added: [AST.Condition] = []
    for fact in facts {
      guard isPushable(fact), case .simple(let op, .column(let name), let right) = fact else {
        continue
      }
      for i in 0..<parentField.count {
        if parentField[i] == name
          && haveSameScalarType(
            columnsOf(parentTable)[parentField[i]], columnsOf(subquery.table)[childField[i]])
        {
          let copy = AST.Condition.simple(op: op, left: .column(childField[i]), right: right)
          if !conjuncts.contains(where: { isSameCondition($0, copy) }) {
            conjuncts.append(copy)
            added.append(copy)
          }
        }
      }
    }
    if added.isEmpty { return subquery }
    var result = subquery
    result.where = simplifyCondition(.and(subquery.where.map { [$0] + added } ?? added))
    return result
  }

  return visit(ast)
}

/// The simple conjuncts of `c`, including those of nested ANDs.
private func simpleConjuncts(_ c: AST.Condition) -> [AST.Condition] {
  switch c {
  case .simple: [c]
  case .and(let cs): cs.flatMap(simpleConjuncts)
  default: []
  }
}

private func isPushable(_ c: AST.Condition) -> Bool {
  guard case .simple(let op, .column, .literal(let value)) = c else { return false }
  switch op {
  case .eq, .is: return true
  case .in:
    if case .array(let a) = value { return a.count <= maxPushedInValues }
    return false
  default: return false
  }
}

private func isSameCondition(_ a: AST.Condition, _ b: AST.Condition) -> Bool {
  guard case .simple(let opA, .column(let nameA), .literal(let va)) = a,
    case .simple(let opB, .column(let nameB), .literal(let vb)) = b
  else { return false }
  return opA == opB && nameA == nameB && va == vb
}

private func haveSameScalarType(_ parent: ColumnSchema?, _ child: ColumnSchema?) -> Bool {
  guard let parent, let child else { return false }
  return parent.type == child.type && parent.type != .json
}
