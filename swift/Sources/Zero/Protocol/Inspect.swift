/// The body of an `inspect` request from the client
/// (`inspectUpBodySchema` in `packages/zero-protocol/src/inspect-up.ts`).
///
/// It is kept as JSON, checked and stripped like valita does: the Swift
/// client has no inspector yet.
package struct InspectUpBody: Sendable, Hashable, JSONCodable {
  package let json: JSONValue

  package init(json: JSONValue) throws(DecodingError) {
    self.json = try InspectSchemas.up.parse(json)
  }

  /// The request ID the response echoes.
  package var id: String { json["id"]?.stringValue ?? "" }
  /// `queries`, `metrics`, `version`, `authenticate`, or `analyze-query`.
  package var op: String { json["op"]?.stringValue ?? "" }
}

/// The body of an `inspect` response from zero-cache
/// (`inspectDownBodySchema` in `packages/zero-protocol/src/inspect-down.ts`).
///
/// It is kept as JSON, checked and stripped like valita does: the Swift
/// client has no inspector yet.
package struct InspectDownBody: Sendable, Hashable, JSONCodable {
  package let json: JSONValue

  package init(json: JSONValue) throws(DecodingError) {
    self.json = try InspectSchemas.down.parse(json)
  }

  /// The ID of the request this answers.
  package var id: String { json["id"]?.stringValue ?? "" }
  /// `queries`, `metrics`, `version`, `authenticated`, `analyze-query`, or
  /// `error`.
  package var op: String { json["op"]?.stringValue ?? "" }
  /// The response, whose shape depends on ``op``.
  package var value: JSONValue { json["value"] ?? .null }
}

/// The inspect schemas of `packages/zero-protocol/src/inspect-up.ts`,
/// `inspect-down.ts`, and `analyze-query-result.ts`.
enum InspectSchemas {
  typealias S = ValitaSchema

  private static func op(_ name: String, _ fields: [String: S.Field] = [:]) -> S {
    .object(fields.merging(["id": .required(.string), "op": .required(.literal(name))]) { $1 })
  }

  private static let ast = S.custom { (v) throws(DecodingError) in try AST(json: v).json }
  private static let strictAST = S.custom { (v) throws(DecodingError) in
    try StrictAST.decode(v).json
  }
  private static let condition = S.custom { (v) throws(DecodingError) in
    try AST.Condition(json: v).json
  }
  private static let ordering = S.array(
    .tuple([.string, .union([.literal("asc"), .literal("desc")])]))
  /// `tdigestSchema` (`packages/shared/src/tdigest-schema.ts`).
  private static let tdigest = S.nonEmptyArray(.number)
  private static let row = S.record(.any)

  static let up = S.union([
    op("queries", ["clientID": .optional(.string)]),
    op("metrics"),
    op("version"),
    op("authenticate", ["value": .required(.string)]),
    op(
      "analyze-query",
      [
        "value": .optional(strictAST),
        "options": .optional(
          .object([
            "vendedRows": .optional(.boolean), "syncedRows": .optional(.boolean),
            "joinPlans": .optional(.boolean),
          ])),
        "ast": .optional(strictAST),
        "name": .optional(.string),
        "args": .optional(.array(.any)),
      ]),
  ])

  private static let queryServerMetrics = S.object([
    "query-hydration-server-ms": .optional(.number),
    "query-update-server": .required(tdigest),
  ])

  private static let inspectQueryRow = S.object([
    "clientID": .required(.string),
    "queryID": .required(.string),
    "ast": .required(.nullable(ast)),
    "name": .required(.nullable(.string)),
    "args": .required(.nullable(.array(.any))),
    "got": .required(.boolean),
    "deleted": .required(.boolean),
    "ttl": .required(.number),
    "inactivatedAt": .required(.nullable(.number)),
    "rowCount": .required(.number),
    "metrics": .optional(.nullable(queryServerMetrics)),
  ])

  private static let serverMetrics = S.object([
    "query-materialization-server": .required(tdigest),
    "query-update-server": .required(tdigest),
  ])

  private static let rowCountsBySource = S.record(.record(.number))
  private static let rowsBySource = S.record(.record(.array(row)))

  private static let costEstimate = S.object([
    "startupCost": .required(.number),
    "scanEst": .required(.number),
    "cost": .required(.number),
    "returnedRows": .required(.number),
    "selectivity": .required(.number),
    "limit": .optional(.number),
  ])

  /// `plannerConstraintSchema`, a record of `v.union(v.unknown(), v.null())`.
  private static let plannerConstraint = S.record(.any)
  private static let constraints = S.record(.nullable(plannerConstraint))

  private static func event(_ type: String, _ fields: [String: S.Field]) -> S {
    .object(fields.merging(["type": .required(.literal(type))]) { $1 })
  }

  private static let joinStates = S.array(
    .object(["join": .required(.string), "type": .required(.string)]))

  private static let planDebugEvent = S.union([
    event(
      "attempt-start",
      ["attemptNumber": .required(.number), "totalAttempts": .required(.number)]),
    event(
      "connection-costs",
      [
        "attemptNumber": .required(.number),
        "costs": .required(
          .array(
            .object([
              "connection": .required(.string),
              "cost": .required(.number),
              "costEstimate": .required(costEstimate),
              "pinned": .required(.boolean),
              "constraints": .required(constraints),
              "constraintCosts": .required(.record(costEstimate)),
            ]))),
      ]),
    event(
      "connection-selected",
      [
        "attemptNumber": .required(.number), "connection": .required(.string),
        "cost": .required(.number), "isRoot": .required(.boolean),
      ]),
    event(
      "constraints-propagated",
      [
        "attemptNumber": .required(.number),
        "connectionConstraints": .required(
          .array(
            .object([
              "connection": .required(.string),
              "constraints": .required(constraints),
              "constraintCosts": .required(.record(costEstimate)),
            ]))),
      ]),
    event(
      "plan-complete",
      [
        "attemptNumber": .required(.number), "totalCost": .required(.number),
        "flipPattern": .required(.number), "joinStates": .required(joinStates),
      ]),
    event(
      "plan-failed", ["attemptNumber": .required(.number), "reason": .required(.string)]),
    event(
      "best-plan-selected",
      [
        "bestAttemptNumber": .required(.number), "totalCost": .required(.number),
        "flipPattern": .required(.number), "joinStates": .required(joinStates),
      ]),
    event(
      "node-cost",
      [
        "attemptNumber": .optional(.number),
        "nodeType": .required(.string),
        "node": .required(.string),
        "branchPattern": .required(.array(.number)),
        "downstreamChildSelectivity": .required(.number),
        "costEstimate": .required(costEstimate),
        "filters": .optional(condition),
        "ordering": .optional(ordering),
        "joinType": .optional(.string),
      ]),
    event(
      "node-constraint",
      [
        "attemptNumber": .optional(.number),
        "nodeType": .required(.string),
        "node": .required(.string),
        "branchPattern": .required(.array(.number)),
        "constraint": .optional(.nullable(plannerConstraint)),
        "from": .required(.string),
      ]),
  ])

  /// `analyzeQueryResultSchema`.
  private static let analyzeQueryResult = S.object([
    "warnings": .required(.array(.string)),
    "syncedRows": .optional(.record(.array(row))),
    "syncedRowCount": .required(.number),
    "start": .required(.number),
    "end": .required(.number),
    "elapsed": .optional(.number),
    "afterPermissions": .optional(.string),
    "vendedRowCounts": .optional(rowCountsBySource),
    "vendedRows": .optional(rowsBySource),
    "sqlitePlans": .optional(.record(.array(.string))),
    "readRows": .optional(rowsBySource),
    "readRowCountsByQuery": .optional(rowCountsBySource),
    "readRowCount": .optional(.number),
    "dbScansByQuery": .optional(rowCountsBySource),
    "joinPlans": .optional(.array(planDebugEvent)),
  ])

  static let down = S.union([
    op("queries", ["value": .required(.array(inspectQueryRow))]),
    op("metrics", ["value": .required(serverMetrics)]),
    op("version", ["value": .required(.string)]),
    op("authenticated", ["value": .required(.boolean)]),
    op("analyze-query", ["value": .required(analyzeQueryResult)]),
    op("error", ["value": .required(.string)]),
  ])
}
