import {must} from 'shared/src/must.js';
import type {
  AST,
  Aggregation,
  Condition,
  Join,
  SimpleCondition,
  Ordering,
  Selector,
  HavingCondition,
  SimpleHavingCondition,
} from '../ast/ast.js';
import {DifferenceStream, concat} from '../ivm/graph/difference-stream.js';
import type {Source} from '../ivm/source/source.js';
import {getValueFromEntity} from '../ivm/source/util.js';
import type {StringOrNumber} from '../ivm/types.js';
import type {Entity} from '../schema/entity-schema.js';

export function buildPipeline(
  sourceProvider: (
    sourceName: string,
    order: Ordering | undefined,
  ) => Source<Entity>,
  ast: AST,
) {
  let {stream} = sourceProvider(
    must(ast.table, 'Table not specified in the AST'),
    ast.orderBy,
  );

  // TODO: start working on pipeline sharing so we don't have to
  // re-build the join index every time.
  if (ast.joins) {
    stream = applyJoins(sourceProvider, ast.table, stream, ast.joins);
  }

  if (ast.where) {
    stream = applyWhere(stream, ast.where);
  }

  let ret: DifferenceStream<Entity> = stream;
  // groupBy also applies aggregations
  if (ast.groupBy) {
    ret = applyGroupBy(
      ret as DifferenceStream<Entity>,
      ast.groupBy,
      ast.aggregate ?? [],
    ) as unknown as DifferenceStream<Entity>;
  }
  // if there was no group-by then we could be aggregating the entire table
  else if (ast.aggregate) {
    ret = applyFullTableAggregation(
      ret as DifferenceStream<Entity>,
      ast.aggregate,
    );
  }

  if (ast.having) {
    ret = applyWhere(ret, ast.having);
  }

  if (ast.distinct) {
    ret = applyDistinct(ret, ast.distinct);
  }

  // Note: the stream is technically attached at this point.
  // We could detach it until the user actually runs (or subscribes to) the statement as a tiny optimization.
  return ret;
}

export function applyJoins<T extends Entity, O extends Entity>(
  sourceProvider: (
    sourceName: string,
    order: Ordering | undefined,
  ) => Source<Entity>,
  sourceTableOrAlias: string,
  stream: DifferenceStream<T>,
  joins: Join[],
): DifferenceStream<O> {
  let ret: DifferenceStream<Entity> =
    stream as unknown as DifferenceStream<Entity>;
  for (const join of joins) {
    const bPipeline = buildPipeline(sourceProvider, join.other);

    const aQualifiedColumn = join.on[0];
    const bQualifiedColumn = join.on[1];
    const joinArgs = {
      aTable: sourceTableOrAlias,
      aPrimaryKeyColumns: ['id'],
      aJoinColumn: aQualifiedColumn,

      b: bPipeline,
      bAs: join.as,
      bTable: join.other.table,
      // TODO(mlaw): either disallow joining against queries or test this for that case.
      bPrimaryKeyColumns: ['id'],
      bJoinColumn: bQualifiedColumn,
    } as const;
    switch (join.type) {
      case 'inner':
        ret = ret.join(joinArgs) as unknown as DifferenceStream<Entity>;
        break;
      case 'left':
        ret = ret.leftJoin(joinArgs) as unknown as DifferenceStream<Entity>;
        break;
    }
  }
  return ret as unknown as DifferenceStream<O>;
}

function applyWhere<T extends Entity>(
  stream: DifferenceStream<T>,
  where: Condition | HavingCondition,
) {
  // We'll handle `OR` and parentheticals like so:
  // OR: We'll create a new stream for the LHS and RHS of the OR then merge together.
  // Parentheticals: We'll create a new stream for the LHS and RHS of the operator involved in combining the parenthetical then merge together.
  //
  // Example:
  // (a = 1 AND b = 2) OR (c = 3 AND d = 4)
  // Becomes
  //       s
  //      / \
  //    a=1 c=3
  //    /     \
  //    b=2   d=4
  //     \    /
  //       OR
  //        |
  //
  // So `ORs` cause a fork (two branches that need to be evaluated) and then that fork is combined.

  switch (where.op) {
    case 'AND':
      return applyAnd(stream, where.conditions);
    case 'OR':
      return applyOr(stream, where.conditions);
    default:
      return applySimpleCondition(stream, where);
  }
}

function applyAnd<T extends Entity>(
  stream: DifferenceStream<T>,
  conditions: (Condition | HavingCondition)[],
) {
  for (const condition of conditions) {
    stream = applyWhere(stream, condition);
  }
  return stream;
}

function applyOr<T extends Entity>(
  stream: DifferenceStream<T>,
  conditions: (Condition | HavingCondition)[],
): DifferenceStream<T> {
  // Or is done by branching the stream and then applying the conditions to each
  // branch. Then we merge the branches back together. At this point we need to
  // ensure we do not get duplicate entries so we add a distinct operator
  const branches = conditions.map(c => applyWhere(stream, c));
  return concat(branches).distinct();
}

function applySimpleCondition<T extends Entity>(
  stream: DifferenceStream<T>,
  condition: SimpleCondition | SimpleHavingCondition,
) {
  return stream.filter(condition.field, condition.op, condition.value.value);
}

function applyDistinct<T extends Entity>(
  stream: DifferenceStream<T>,
  column: Selector,
) {
  return stream.distinctAll(
    x => getValueFromEntity(x, column) as StringOrNumber,
  );
}

function applyGroupBy<T extends Entity>(
  stream: DifferenceStream<T>,
  columns: Selector[],
  aggregations: Aggregation[],
) {
  const qualifiedColumns = aggregations.map(q =>
    q.field === undefined ? undefined : q.field,
  );

  return stream.reduce(
    columns,
    value => value.id as string,
    values => {
      const first = values[Symbol.iterator]().next().value;
      const ret: Record<string, unknown> = {...first};

      for (let i = 0; i < aggregations.length; i++) {
        const aggregation = aggregations[i];
        const qualifiedColumn = qualifiedColumns[i];
        switch (aggregation.aggregate) {
          case 'count': {
            let count = 0;
            for (const _ of values) {
              count++;
            }
            ret[aggregation.alias] = count;
            break;
          }
          case 'sum': {
            let sum = 0;
            for (const value of values) {
              sum += (getValueFromEntity(value, must(qualifiedColumn)) ??
                0) as number;
            }
            ret[aggregation.alias] = sum;
            break;
          }
          case 'avg': {
            let sum = 0;
            let count = 0;
            for (const value of values) {
              const v = getValueFromEntity(value, must(qualifiedColumn));
              if (v === undefined) {
                continue;
              }
              sum += v as number;
              count++;
            }
            ret[aggregation.alias] = sum / count;
            break;
          }
          case 'min': {
            let min;
            for (const value of values) {
              const newValue = getValueFromEntity(
                value,
                must(qualifiedColumn),
              ) as number | string;
              if (newValue === undefined) {
                continue;
              }
              if (min === undefined || min === null || min > newValue) {
                min = newValue;
              }
            }
            ret[aggregation.alias] = min;
            break;
          }
          case 'max': {
            let max;
            for (const value of values) {
              const newValue = getValueFromEntity(
                value,
                must(qualifiedColumn),
              ) as number | string;
              if (newValue === undefined) {
                continue;
              }
              if (max === undefined || max === null || max < newValue) {
                max = newValue;
              }
            }
            ret[aggregation.alias] = max;
            break;
          }
          case 'array': {
            const arr: unknown[] = [];
            for (const value of values) {
              const extracted = getValueFromEntity(
                value,
                must(qualifiedColumn),
              );
              if (extracted !== undefined) {
                arr.push(extracted);
              }
            }
            ret[aggregation.alias] = arr;
            break;
          }
          default:
            throw new Error(`Unknown aggregation ${aggregation.aggregate}`);
        }
      }
      return ret;
    },
  );
}

function applyFullTableAggregation<T extends Entity>(
  stream: DifferenceStream<T>,
  aggregations: Aggregation[],
) {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let ret: DifferenceStream<any> = stream;
  for (const agg of aggregations) {
    switch (agg.aggregate) {
      case 'array':
      case 'min':
      case 'max':
        throw new Error(
          `${agg.aggregate} not yet supported outside of group-by`,
        );
      case 'avg':
        ret = ret.average(must(agg.field), agg.alias);
        break;
      case 'count':
        ret = ret.count(agg.alias);
        break;
      case 'sum':
        ret = ret.sum(must(agg.field), agg.alias);
        break;
    }
  }

  return ret;
}
