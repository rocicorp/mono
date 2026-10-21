/// The schema of an operator's output (`packages/zql/src/ivm/schema.ts`).
package final class SourceSchema {
  package let tableName: String
  package let columns: [String: ColumnSchema]
  package let primaryKey: [String]
  /// Relationship schemas, in the order they were added.
  package let relationships: [(name: String, schema: SourceSchema)]
  package let isHidden: Bool
  package let system: AST.System
  package let compareRows: Comparator
  /// `nil` for an unordered input.
  package let sort: AST.Ordering?

  package init(
    tableName: String,
    columns: [String: ColumnSchema],
    primaryKey: [String],
    relationships: [(name: String, schema: SourceSchema)] = [],
    isHidden: Bool = false,
    system: AST.System = .client,
    compareRows: @escaping Comparator,
    sort: AST.Ordering?
  ) {
    self.tableName = tableName
    self.columns = columns
    self.primaryKey = primaryKey
    self.relationships = relationships
    self.isHidden = isHidden
    self.system = system
    self.compareRows = compareRows
    self.sort = sort
  }

  package func relationship(_ name: String) -> SourceSchema? {
    for r in relationships where r.name == name { return r.schema }
    return nil
  }

  package var relationshipNames: [String] { relationships.map(\.name) }

  /// A copy with the given fields replaced.
  func with(
    relationships: [(name: String, schema: SourceSchema)]? = nil,
    isHidden: Bool? = nil,
    system: AST.System? = nil
  ) -> SourceSchema {
    SourceSchema(
      tableName: tableName, columns: columns, primaryKey: primaryKey,
      relationships: relationships ?? self.relationships, isHidden: isHidden ?? self.isHidden,
      system: system ?? self.system, compareRows: compareRows, sort: sort)
  }

  /// This schema with `schema` added (or replaced) as relationship `name`.
  func adding(_ name: String, _ schema: SourceSchema) -> SourceSchema {
    var rels = relationships
    if let i = rels.firstIndex(where: { $0.name == name }) {
      rels[i].schema = schema
    } else {
      rels.append((name, schema))
    }
    return with(relationships: rels)
  }
}

/// Where a fetch starts (`Start` in `operator.ts`).
package struct Start {
  package enum Basis: Sendable { case at, after }
  package var row: Row
  package var basis: Basis

  package init(row: Row, basis: Basis) {
    self.row = row
    self.basis = basis
  }
}

/// `FetchRequest` in `operator.ts`.
package struct FetchRequest {
  package var constraint: Constraint?
  /// Multi-row IN clauses, all ANDed together and with `constraint`.
  package var multiConstraints: [[Constraint]]?
  /// If supplied, `start.row` must have previously been output by fetch or
  /// push.
  package var start: Start?
  /// Whether to fetch in reverse order of the schema's sort.
  package var reverse: Bool
  /// A predicate without subqueries for the source to apply. Only
  /// `FilterStart` introduces one; pass-through operators keep it.
  package var filter: AST.Condition?

  package init(
    constraint: Constraint? = nil,
    multiConstraints: [[Constraint]]? = nil,
    start: Start? = nil,
    reverse: Bool = false,
    filter: AST.Condition? = nil
  ) {
    self.constraint = constraint
    self.multiConstraints = multiConstraints
    self.start = start
    self.reverse = reverse
    self.filter = filter
  }
}

/// `InputBase` in `operator.ts`.
package protocol InputBase: AnyObject {
  func getSchema() -> SourceSchema
  func destroy()
}

/// An operator's input: something that can be fetched from and pushes to an
/// output.
package protocol Input: InputBase {
  func setOutput(_ output: any Output)
  func fetch(_ req: FetchRequest) -> NodeStream
}

/// An operator's output (`Output` in `operator.ts`).
///
/// Pushing is two-phase. `push` propagates a change; once every change of a
/// source push has propagated, `reconcile` lets bounded operators (`Take`)
/// refill and push the rows that refill them.
package protocol Output: AnyObject {
  func push(_ change: Change, pusher: any InputBase)
  /// Phase two. Sinks can ignore it; operators must forward it.
  func reconcile(pusher: any InputBase)
}

extension Output {
  package func reconcile(pusher: any InputBase) {}
}

/// An operator in a pipeline: an input to the next and an output of the
/// previous.
///
/// Operators hold their inputs strongly and their outputs weakly, so the
/// view at the end of a pipeline owns it. A push that reaches a released
/// output is dropped.
package protocol Operator: Input, Output {}

/// Routes an input's pushes to closures, as JS operators do with object
/// literals. The operator owns it: inputs hold their outputs weakly.
final class ClosureOutput: Output {
  let onPush: (Change) -> Void
  let onReconcile: () -> Void

  init(push: @escaping (Change) -> Void, reconcile: @escaping () -> Void) {
    onPush = push
    onReconcile = reconcile
  }

  func push(_ change: Change, pusher: any InputBase) { onPush(change) }
  func reconcile(pusher: any InputBase) { onReconcile() }
}

/// The `where` sub-graph's input (`FilterInput` in `filter-operators.ts`).
package protocol FilterInput: InputBase {
  func setFilterOutput(_ output: any FilterOutput)
}

/// The `where` sub-graph's output. Filter operators have no `fetch`; a fetch
/// through the sub-graph asks `filter` whether each node passes.
package protocol FilterOutput: Output {
  func beginFilter()
  func filter(_ node: Node) -> Bool
  func endFilter()
}

package protocol FilterOperator: FilterInput, FilterOutput {}

/// A source of rows for pipelines (`Source` in `source.ts`).
package protocol Source: AnyObject {
  var tableSchema: TableSchema { get }
  /// Connects a pipeline. `sort` is `nil` for an unordered connection.
  func connect(
    sort: AST.Ordering?, filters: AST.Condition?, splitEditKeys: Set<String>?
  ) throws(PipelineError) -> any SourceInput
  func push(_ change: SourceChange)
}

/// A source's connection to one pipeline.
package protocol SourceInput: Input {
  /// Whether the source applies all of the connection's filters, so no
  /// `Filter` operators are needed above it.
  var fullyAppliedFilters: Bool { get }
}
