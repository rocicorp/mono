import {assert} from '../../../shared/src/asserts.ts';
import type {JSONValue} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import type {
  AST,
  ColumnReference,
  CompoundKey,
  Condition,
  Conjunction,
  CorrelatedSubquery,
  CorrelatedSubqueryCondition,
  Disjunction,
  LiteralValue,
  Ordering,
  Parameter,
  SimpleCondition,
  ValuePosition,
} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import {Exists} from '../ivm/exists.ts';
import {FanIn} from '../ivm/fan-in.ts';
import {FanOut} from '../ivm/fan-out.ts';
import {
  buildFilterPipeline,
  type FilterInput,
} from '../ivm/filter-operators.ts';
import {Filter} from '../ivm/filter.ts';
import {Join} from '../ivm/join.ts';
import type {Input, InputBase, Storage} from '../ivm/operator.ts';
import {Skip} from '../ivm/skip.ts';
import type {Source, SourceInput} from '../ivm/source.ts';
import {Take} from '../ivm/take.ts';
import type {DebugDelegate} from './debug-delegate.ts';
import {createPredicate, type NoSubqueryCondition} from './filter.ts';

export type StaticQueryParameters = {
  authData: Record<string, JSONValue>;
  preMutationRow?: Row | undefined;
};

/**
 * Interface required of caller to buildPipeline. Connects to constructed
 * pipeline to delegate environment to provide sources and storage.
 */
export interface BuilderDelegate {
  readonly applyFiltersAnyway?: boolean | undefined;
  readonly debug?: DebugDelegate | undefined;

  /**
   * Called once for each source needed by the AST.
   * Might be called multiple times with same tableName. It is OK to return
   * same storage instance in that case.
   */
  getSource(tableName: string): Source | undefined;

  /**
   * Called once for each operator that requires storage. Should return a new
   * unique storage object for each call.
   */
  createStorage(name: string): Storage;

  decorateInput(input: Input, name: string): Input;

  addEdge(source: InputBase, dest: InputBase): void;

  decorateFilterInput(input: FilterInput, name: string): FilterInput;

  decorateSourceInput(input: SourceInput, queryID: string): Input;

  /**
   * The AST is mapped on-the-wire between client and server names.
   *
   * There is no "wire" for zqlite tests so this function is provided
   * to allow tests to remap the AST.
   */
  mapAst?: ((ast: AST) => AST) | undefined;
}

/**
 * Builds a pipeline from an AST. Caller must provide a delegate to create source
 * and storage interfaces as necessary.
 *
 * Usage:
 *
 * ```ts
 * class MySink implements Output {
 *   readonly #input: Input;
 *
 *   constructor(input: Input) {
 *     this.#input = input;
 *     input.setOutput(this);
 *   }
 *
 *   push(change: Change, _: Operator) {
 *     console.log(change);
 *   }
 * }
 *
 * const input = buildPipeline(ast, myDelegate, hash(ast));
 * const sink = new MySink(input);
 * ```
 */
export function buildPipeline(
  ast: AST,
  delegate: BuilderDelegate,
  queryID: string,
): Input {
  // Apply mapAst if provided
  const mappedAst = delegate.mapAst ? delegate.mapAst(ast) : ast;

  // Uniquify all correlated subquery aliases across the entire AST tree
  const uniquifiedAst = uniquifyCorrelatedSubqueryConditionAliases(mappedAst);

  // Build the pipeline with the transformed AST
  return buildPipelineInternal(uniquifiedAst, delegate, queryID, '');
}

export function bindStaticParameters(
  ast: AST,
  staticQueryParameters: StaticQueryParameters | undefined,
) {
  const visit = (node: AST): AST => ({
    ...node,
    where: node.where ? bindCondition(node.where) : undefined,
    related: node.related?.map(sq => ({
      ...sq,
      subquery: visit(sq.subquery),
    })),
  });

  function bindCondition(condition: Condition): Condition {
    if (condition.type === 'simple') {
      return {
        ...condition,
        left: bindValue(condition.left),
        right: bindValue(condition.right) as Exclude<
          ValuePosition,
          ColumnReference
        >,
      };
    }
    if (condition.type === 'correlatedSubquery') {
      return {
        ...condition,
        related: {
          ...condition.related,
          subquery: visit(condition.related.subquery),
        },
      };
    }
    return {
      ...condition,
      conditions: condition.conditions.map(bindCondition),
    };
  }

  const bindValue = (value: ValuePosition): ValuePosition => {
    if (isParameter(value)) {
      const anchor = must(
        staticQueryParameters,
        'Static query params do not exist',
      )[value.anchor];
      const resolvedValue = resolveField(anchor, value.field);
      return {
        type: 'literal',
        value: resolvedValue as LiteralValue,
      };
    }
    return value;
  };

  return visit(ast);
}

function resolveField(
  anchor: Record<string, JSONValue> | Row | undefined,
  field: string | string[],
): unknown {
  if (anchor === undefined) {
    return null;
  }

  if (Array.isArray(field)) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return field.reduce((acc, f) => (acc as any)?.[f], anchor) ?? null;
  }

  return anchor[field] ?? null;
}

function isParameter(value: ValuePosition): value is Parameter {
  return value.type === 'static';
}

function buildPipelineInternal(
  ast: AST,
  delegate: BuilderDelegate,
  queryID: string,
  name: string,
  partitionKey?: CompoundKey | undefined,
): Input {
  const source = delegate.getSource(ast.table);
  if (!source) {
    throw new Error(`Source not found: ${ast.table}`);
  }

  const csqsFromCondition = gatherCorrelatedSubqueryQueriesFromCondition(
    ast.where,
  );
  const splitEditKeys: Set<string> = partitionKey
    ? new Set(partitionKey)
    : new Set();
  const aliases = new Set<string>();
  for (const csq of csqsFromCondition) {
    aliases.add(csq.subquery.alias || '');
    for (const key of csq.correlation.parentField) {
      splitEditKeys.add(key);
    }
  }
  if (ast.related) {
    for (const csq of ast.related) {
      for (const key of csq.correlation.parentField) {
        splitEditKeys.add(key);
      }
    }
  }
  const conn = source.connect(
    must(ast.orderBy),
    ast.where,
    splitEditKeys,
    delegate.debug,
  );

  let end: Input = delegate.decorateSourceInput(conn, queryID);
  end = delegate.decorateInput(end, `${name}:source(${ast.table})`);
  const {fullyAppliedFilters} = conn;

  if (ast.start) {
    const skip = new Skip(end, ast.start);
    delegate.addEdge(end, skip);
    end = delegate.decorateInput(skip, `${name}:skip)`);
  }

  for (const csq of csqsFromCondition) {
    end = applyCorrelatedSubQuery(csq, delegate, queryID, end, name, true);
  }

  if (ast.where && (!fullyAppliedFilters || delegate.applyFiltersAnyway)) {
    end = applyWhere(end, ast.where, delegate, name);
  }

  if (ast.limit !== undefined) {
    const takeName = `${name}:take`;
    const take = new Take(
      end,
      delegate.createStorage(takeName),
      ast.limit,
      partitionKey,
    );
    delegate.addEdge(end, take);
    end = delegate.decorateInput(take, takeName);
  }

  if (ast.related) {
    for (const csq of ast.related) {
      end = applyCorrelatedSubQuery(csq, delegate, queryID, end, name, false);
    }
  }

  return end;
}

function applyWhere(
  input: Input,
  condition: Condition,
  delegate: BuilderDelegate,
  name: string,
): Input {
  return buildFilterPipeline(input, delegate, filterInput =>
    applyFilter(filterInput, condition, delegate, name),
  );
}

function applyFilter(
  input: FilterInput,
  condition: Condition,
  delegate: BuilderDelegate,
  name: string,
) {
  switch (condition.type) {
    case 'and':
      return applyAnd(input, condition, delegate, name);
    case 'or':
      return applyOr(input, condition, delegate, name);
    case 'correlatedSubquery':
      return applyCorrelatedSubqueryCondition(input, condition, delegate, name);
    case 'simple':
      return applySimpleCondition(input, delegate, condition);
  }
}

function applyAnd(
  input: FilterInput,
  condition: Conjunction,
  delegate: BuilderDelegate,
  name: string,
): FilterInput {
  for (const subCondition of condition.conditions) {
    input = applyFilter(input, subCondition, delegate, name);
  }
  return input;
}

export function applyOr(
  input: FilterInput,
  condition: Disjunction,
  delegate: BuilderDelegate,
  name: string,
): FilterInput {
  const [subqueryConditions, otherConditions] =
    groupSubqueryConditions(condition);
  // if there are no subquery conditions, no fan-in / fan-out is needed
  if (subqueryConditions.length === 0) {
    const filter = new Filter(
      input,
      createPredicate({
        type: 'or',
        conditions: otherConditions,
      }),
    );
    delegate.addEdge(input, filter);
    return filter;
  }

  const fanOut = new FanOut(input);
  delegate.addEdge(input, fanOut);
  const branches = subqueryConditions.map(subCondition =>
    applyFilter(fanOut, subCondition, delegate, name),
  );
  if (otherConditions.length > 0) {
    const filter = new Filter(
      fanOut,
      createPredicate({
        type: 'or',
        conditions: otherConditions,
      }),
    );
    delegate.addEdge(fanOut, filter);
    branches.push(filter);
  }
  const ret = new FanIn(fanOut, branches);
  for (const branch of branches) {
    delegate.addEdge(branch, ret);
  }
  fanOut.setFanIn(ret);
  return ret;
}

export function groupSubqueryConditions(condition: Disjunction) {
  const partitioned: [
    subqueryConditions: Condition[],
    otherConditions: NoSubqueryCondition[],
  ] = [[], []];
  for (const subCondition of condition.conditions) {
    if (isNotAndDoesNotContainSubquery(subCondition)) {
      partitioned[1].push(subCondition);
    } else {
      partitioned[0].push(subCondition);
    }
  }
  return partitioned;
}

export function isNotAndDoesNotContainSubquery(
  condition: Condition,
): condition is NoSubqueryCondition {
  if (condition.type === 'correlatedSubquery') {
    return false;
  }
  if (condition.type === 'simple') {
    return true;
  }
  return condition.conditions.every(isNotAndDoesNotContainSubquery);
}

function applySimpleCondition(
  input: FilterInput,
  delegate: BuilderDelegate,
  condition: SimpleCondition,
): FilterInput {
  const filter = new Filter(input, createPredicate(condition));
  delegate.decorateFilterInput(
    filter,
    `${valuePosName(condition.left)}:${condition.op}:${valuePosName(condition.right)}`,
  );
  delegate.addEdge(input, filter);
  return filter;
}

function valuePosName(left: ValuePosition) {
  switch (left.type) {
    case 'static':
      return left.field;
    case 'literal':
      return left.value;
    case 'column':
      return left.name;
  }
}

function applyCorrelatedSubQuery(
  sq: CorrelatedSubquery,
  delegate: BuilderDelegate,
  queryID: string,
  end: Input,
  name: string,
  fromCondition: boolean,
) {
  // TODO: we only omit the join if the CSQ if from a condition since
  // we want to create an empty array for `related` fields that are `limit(0)`
  if (sq.subquery.limit === 0 && fromCondition) {
    return end;
  }

  assert(sq.subquery.alias, 'Subquery must have an alias');

  const child = buildPipelineInternal(
    sq.subquery,
    delegate,
    queryID,
    `${name}.${sq.subquery.alias}`,
    sq.correlation.childField,
  );
  const joinName = `${name}:join(${sq.subquery.alias})`;
  const join = new Join({
    parent: end,
    child,
    storage: delegate.createStorage(joinName),
    parentKey: sq.correlation.parentField,
    childKey: sq.correlation.childField,
    relationshipName: sq.subquery.alias,
    hidden: sq.hidden ?? false,
    system: sq.system ?? 'client',
  });
  delegate.addEdge(end, join);
  delegate.addEdge(child, join);
  return delegate.decorateInput(join, joinName);
}

function applyCorrelatedSubqueryCondition(
  input: FilterInput,
  condition: CorrelatedSubqueryCondition,
  delegate: BuilderDelegate,
  name: string,
): FilterInput {
  assert(condition.op === 'EXISTS' || condition.op === 'NOT EXISTS');
  if (condition.related.subquery.limit === 0) {
    if (condition.op === 'EXISTS') {
      const filter = new Filter(input, () => false);
      delegate.addEdge(input, filter);
      return filter;
    }
    const filter = new Filter(input, () => true);
    delegate.addEdge(input, filter);
    return filter;
  }
  const existsName = `${name}:exists(${condition.related.subquery.alias})`;
  const exists = new Exists(
    input,
    delegate.createStorage(existsName),
    must(condition.related.subquery.alias),
    condition.related.correlation.parentField,
    condition.op,
  );
  delegate.addEdge(input, exists);
  return delegate.decorateFilterInput(exists, existsName);
}

function gatherCorrelatedSubqueryQueriesFromCondition(
  condition: Condition | undefined,
) {
  const csqs: CorrelatedSubquery[] = [];
  const gather = (condition: Condition) => {
    if (condition.type === 'correlatedSubquery') {
      assert(condition.op === 'EXISTS' || condition.op === 'NOT EXISTS');

      csqs.push({
        ...condition.related,
        subquery: {
          ...condition.related.subquery,
          limit:
            condition.related.system === 'permissions'
              ? PERMISSIONS_EXISTS_LIMIT
              : EXISTS_LIMIT,
        },
      });
      return;
    }
    if (condition.type === 'and' || condition.type === 'or') {
      for (const c of condition.conditions) {
        gather(c);
      }
      return;
    }
  };
  if (condition) {
    gather(condition);
  }
  return csqs;
}

const EXISTS_LIMIT = 3;
const PERMISSIONS_EXISTS_LIMIT = 1;

export function assertOrderingIncludesPK(
  ordering: Ordering,
  pk: PrimaryKey,
): void {
  const orderingFields = ordering.map(([field]) => field);
  const missingFields = pk.filter(pkField => !orderingFields.includes(pkField));

  if (missingFields.length > 0) {
    throw new Error(
      `Ordering must include all primary key fields. Missing: ${missingFields.join(
        ', ',
      )}. ZQL automatically appends primary key fields to the ordering if they are missing 
      so a common cause of this error is a casing mismatch between Postgres and ZQL.
      E.g., "userid" vs "userID".
      You may want to add double-quotes around your Postgres column names to prevent Postgres from lower-casing them:
      https://www.postgresql.org/docs/current/sql-syntax-lexical.htm`,
    );
  }
}

function uniquifyCorrelatedSubqueryConditionAliases(ast: AST): AST {
  let count = 0;

  // Process an entire AST recursively
  const processAST = (node: AST): AST => {
    // Process WHERE conditions in this AST node
    const processedWhere = node.where
      ? uniquifyCondition(node.where)
      : undefined;

    return {
      ...node,
      where: processedWhere,
    };
  };

  // Process a condition tree, uniquifying aliases and recursing into subqueries
  const uniquifyCondition = (cond: Condition): Condition => {
    if (cond.type === 'simple') {
      return cond;
    } else if (cond.type === 'correlatedSubquery') {
      // Uniquify the alias for this correlated subquery
      const uniquifiedAlias =
        (cond.related.subquery.alias ?? cond.related.subquery.table) +
        '_' +
        count++;

      // Recursively process the subquery AST
      const processedSubquery = processAST(cond.related.subquery);

      return {
        ...cond,
        related: {
          ...cond.related,
          subquery: {
            ...processedSubquery,
            alias: uniquifiedAlias,
          },
        },
      };
    } else if (cond.type === 'and' || cond.type === 'or') {
      // Process all child conditions
      const conditions = cond.conditions.map(c => uniquifyCondition(c));
      return {
        type: cond.type,
        conditions,
      };
    }
    return cond;
  };

  return processAST(ast);
}
