import {
  deepEqual,
  type ReadonlyJSONValue,
} from '../../../../shared/src/json.ts';
import {
  normalizeAST,
  type AST,
  type Condition,
  type CorrelatedSubquery,
  type CorrelatedSubqueryCondition,
  type LiteralValue,
  type SimpleCondition,
} from '../../../../zero-protocol/src/ast.ts';

export type RunningQuery = {
  readonly transformedAst: AST;
  readonly transformationHash: string;
  readonly queryName?: string | undefined;
};

export type CoveringQuery = {
  readonly queryID: string;
  readonly transformationHash: string;
  readonly queryName?: string | undefined;
};

type NormalizedAST = Required<AST>;
type NonNullScalarLiteralValue = string | number | boolean;
type IndexedRunningQuery = RunningQuery & {
  readonly normalizedAst: NormalizedAST;
};

/**
 * Returns true when every row that can be produced by `covered` is also
 * produced by `covering`.
 *
 * This is intentionally conservative: unsupported cases return false rather
 * than guessing. It is used for shadow logging only.
 */
export function isQueryCoveredBy(covered: AST, covering: AST): boolean {
  return astCoveredBy(normalizeAST(covered), normalizeAST(covering));
}

export function findCoveringQuery(
  coveredQueryID: string,
  coveredAst: AST,
  runningQueries: ReadonlyMap<string, RunningQuery>,
): CoveringQuery | undefined {
  return new QueryCoveringIndex(runningQueries).findCoveringQuery(
    coveredQueryID,
    coveredAst,
  );
}

export class QueryCoveringIndex {
  readonly #byRoot = new Map<string, Map<string, IndexedRunningQuery>>();
  readonly #queryIDToRoot = new Map<string, string>();

  constructor(runningQueries?: ReadonlyMap<string, RunningQuery>) {
    if (runningQueries) {
      for (const [queryID, query] of runningQueries) {
        this.add(queryID, query);
      }
    }
  }

  add(queryID: string, query: RunningQuery): void {
    this.remove(queryID);

    const normalizedAst = normalizeAST(query.transformedAst);
    const root = rootKey(normalizedAst);
    let queries = this.#byRoot.get(root);
    if (!queries) {
      queries = new Map();
      this.#byRoot.set(root, queries);
    }
    queries.set(queryID, {...query, normalizedAst});
    this.#queryIDToRoot.set(queryID, root);
  }

  remove(queryID: string): void {
    const root = this.#queryIDToRoot.get(queryID);
    if (root === undefined) {
      return;
    }

    this.#queryIDToRoot.delete(queryID);
    const queries = this.#byRoot.get(root);
    if (!queries) {
      return;
    }

    queries.delete(queryID);
    if (queries.size === 0) {
      this.#byRoot.delete(root);
    }
  }

  findCoveringQuery(
    coveredQueryID: string,
    coveredAst: AST,
  ): CoveringQuery | undefined {
    const normalizedCoveredAst = normalizeAST(coveredAst);
    const queries = this.#byRoot.get(rootKey(normalizedCoveredAst));
    if (!queries) {
      return undefined;
    }

    for (const [queryID, query] of queries) {
      if (queryID === coveredQueryID) {
        continue;
      }
      if (astCoveredBy(normalizedCoveredAst, query.normalizedAst)) {
        return {
          queryID,
          transformationHash: query.transformationHash,
          ...(query.queryName !== undefined && {queryName: query.queryName}),
        };
      }
    }
    return undefined;
  }
}

function rootKey(ast: NormalizedAST): string {
  return JSON.stringify([ast.schema, ast.table, ast.alias]);
}

function astCoveredBy(
  covered: NormalizedAST,
  covering: NormalizedAST,
): boolean {
  return (
    covered.schema === covering.schema &&
    covered.table === covering.table &&
    covered.alias === covering.alias &&
    conditionImplies(covered.where, covering.where) &&
    relatedCoveredBy(covered.related, covering.related) &&
    boundsCoveredBy(covered, covering)
  );
}

function boundsCoveredBy(
  covered: NormalizedAST,
  covering: NormalizedAST,
): boolean {
  if (covering.limit === undefined) {
    if (covering.start === undefined) {
      return true;
    }
    return (
      jsonEqual(covered.start, covering.start) &&
      jsonEqual(covered.orderBy, covering.orderBy)
    );
  }

  if (covered.limit === undefined || covering.limit < covered.limit) {
    return false;
  }

  // A limited broader query does not necessarily contain a limited narrower
  // query. The ordered input to the limit must be equivalent.
  return (
    conditionEquivalent(covered.where, covering.where) &&
    jsonEqual(covered.start, covering.start) &&
    jsonEqual(covered.orderBy, covering.orderBy)
  );
}

function relatedCoveredBy(
  covered: readonly CorrelatedSubquery[] | undefined,
  covering: readonly CorrelatedSubquery[] | undefined,
): boolean {
  if (covered === undefined || covered.length === 0) {
    return true;
  }
  if (covering === undefined) {
    return false;
  }
  return covered.every(coveredRelated =>
    covering.some(
      coveringRelated =>
        sameRelatedEdge(coveredRelated, coveringRelated) &&
        astCoveredBy(
          coveredRelated.subquery as NormalizedAST,
          coveringRelated.subquery as NormalizedAST,
        ),
    ),
  );
}

function conditionEquivalent(
  a: Condition | undefined,
  b: Condition | undefined,
): boolean {
  return conditionImplies(a, b) && conditionImplies(b, a);
}

function conditionImplies(
  covered: Condition | undefined,
  covering: Condition | undefined,
): boolean {
  if (covering === undefined) {
    return true;
  }
  if (covered === undefined) {
    return false;
  }
  if (jsonEqual(covered, covering)) {
    return true;
  }

  if (covered.type === 'or') {
    return covered.conditions.every(c => conditionImplies(c, covering));
  }
  if (covering.type === 'or') {
    return covering.conditions.some(c => conditionImplies(covered, c));
  }
  if (covering.type === 'and') {
    return covering.conditions.every(c => conditionImplies(covered, c));
  }
  if (covered.type === 'and') {
    return covered.conditions.some(c => conditionImplies(c, covering));
  }
  if (covered.type === 'simple' && covering.type === 'simple') {
    return simpleConditionImplies(covered, covering);
  }
  if (
    covered.type === 'correlatedSubquery' &&
    covering.type === 'correlatedSubquery'
  ) {
    return correlatedConditionImplies(covered, covering);
  }
  return false;
}

function correlatedConditionImplies(
  covered: CorrelatedSubqueryCondition,
  covering: CorrelatedSubqueryCondition,
): boolean {
  if (
    covered.op !== covering.op ||
    covered.scalar !== covering.scalar ||
    !sameRelatedEdge(covered.related, covering.related)
  ) {
    return false;
  }

  if (covered.op === 'EXISTS') {
    return astCoveredBy(
      covered.related.subquery as NormalizedAST,
      covering.related.subquery as NormalizedAST,
    );
  }

  return astCoveredBy(
    covering.related.subquery as NormalizedAST,
    covered.related.subquery as NormalizedAST,
  );
}

function sameRelatedEdge(
  a: CorrelatedSubquery,
  b: CorrelatedSubquery,
): boolean {
  return (
    jsonEqual(a.correlation, b.correlation) &&
    a.hidden === b.hidden &&
    a.system === b.system &&
    a.subquery.alias === b.subquery.alias
  );
}

function simpleConditionImplies(
  covered: SimpleCondition,
  covering: SimpleCondition,
): boolean {
  const coveredParts = columnLiteralParts(covered);
  const coveringParts = columnLiteralParts(covering);
  if (!coveredParts || !coveringParts) {
    return false;
  }
  if (coveredParts.column !== coveringParts.column) {
    return false;
  }

  const {op: coveredOp, value: coveredValue} = coveredParts;
  const {op: coveringOp, value: coveringValue} = coveringParts;

  if (isEqualityOp(coveredOp) && isNonNullScalarLiteralValue(coveredValue)) {
    return equalityImplies(coveredValue, coveringOp, coveringValue);
  }

  if (
    coveredOp === 'IN' &&
    coveringOp === 'IN' &&
    Array.isArray(coveredValue) &&
    Array.isArray(coveringValue)
  ) {
    return coveredValue.every(v => literalArrayIncludes(coveringValue, v));
  }

  if (isNumericOrderOp(coveredOp) && isNumericOrderOp(coveringOp)) {
    return orderConditionImplies(
      coveredOp,
      coveredValue,
      coveringOp,
      coveringValue,
    );
  }

  return false;
}

function equalityImplies(
  value: NonNullScalarLiteralValue,
  coveringOp: SimpleCondition['op'],
  coveringValue: LiteralValue,
): boolean {
  switch (coveringOp) {
    case '=':
    case 'IS':
      return jsonEqual(value, coveringValue);
    case '!=':
    case 'IS NOT':
      return !jsonEqual(value, coveringValue);
    case 'IN':
      return (
        Array.isArray(coveringValue) &&
        literalArrayIncludes(coveringValue, value)
      );
    case '<':
      return (
        typeof value === 'number' &&
        typeof coveringValue === 'number' &&
        value < coveringValue
      );
    case '<=':
      return (
        typeof value === 'number' &&
        typeof coveringValue === 'number' &&
        value <= coveringValue
      );
    case '>':
      return (
        typeof value === 'number' &&
        typeof coveringValue === 'number' &&
        value > coveringValue
      );
    case '>=':
      return (
        typeof value === 'number' &&
        typeof coveringValue === 'number' &&
        value >= coveringValue
      );
    case 'NOT IN':
    case 'LIKE':
    case 'NOT LIKE':
    case 'ILIKE':
    case 'NOT ILIKE':
      return false;
  }
}

function orderConditionImplies(
  coveredOp: '<' | '>' | '<=' | '>=',
  coveredValue: LiteralValue,
  coveringOp: '<' | '>' | '<=' | '>=',
  coveringValue: LiteralValue,
): boolean {
  if (typeof coveredValue !== 'number' || typeof coveringValue !== 'number') {
    return false;
  }

  switch (coveredOp) {
    case '>':
      return (
        (coveringOp === '>' && coveredValue >= coveringValue) ||
        (coveringOp === '>=' && coveredValue >= coveringValue)
      );
    case '>=':
      return (
        (coveringOp === '>' && coveredValue > coveringValue) ||
        (coveringOp === '>=' && coveredValue >= coveringValue)
      );
    case '<':
      return (
        (coveringOp === '<' && coveredValue <= coveringValue) ||
        (coveringOp === '<=' && coveredValue <= coveringValue)
      );
    case '<=':
      return (
        (coveringOp === '<' && coveredValue < coveringValue) ||
        (coveringOp === '<=' && coveredValue <= coveringValue)
      );
  }
}

function columnLiteralParts(condition: SimpleCondition):
  | {
      readonly column: string;
      readonly op: SimpleCondition['op'];
      readonly value: LiteralValue;
    }
  | undefined {
  if (condition.left.type !== 'column' || condition.right.type !== 'literal') {
    return undefined;
  }
  return {
    column: condition.left.name,
    op: condition.op,
    value: condition.right.value,
  };
}

function isEqualityOp(op: SimpleCondition['op']): op is '=' | 'IS' {
  return op === '=' || op === 'IS';
}

function isNumericOrderOp(
  op: SimpleCondition['op'],
): op is '<' | '>' | '<=' | '>=' {
  return op === '<' || op === '>' || op === '<=' || op === '>=';
}

function isNonNullScalarLiteralValue(
  value: LiteralValue,
): value is NonNullScalarLiteralValue {
  return value !== null && !Array.isArray(value);
}

function literalArrayIncludes(
  values: ReadonlyArray<string | number | boolean>,
  value: string | number | boolean | null,
): boolean {
  return values.some(v => jsonEqual(v, value));
}

function jsonEqual(a: unknown, b: unknown): boolean {
  return deepEqual(
    a as ReadonlyJSONValue | undefined,
    b as ReadonlyJSONValue | undefined,
  );
}
