import {Entity} from '../../generate.js';
import {
  AST,
  Aggregation,
  Condition,
  Ordering,
  SimpleCondition,
} from '../ast/ast.js';
import {must} from '../error/asserts.js';
import {DifferenceStream, concat} from '../ivm/graph/difference-stream.js';

export const orderingProp = Symbol();

export function buildPipeline(
  sourceStreamProvider: (sourceName: string) => DifferenceStream<Entity>,
  ast: AST,
) {
  // filters first
  // select last
  // order is a param to the source or view
  // as well as limit? How does limit work in materialite again?
  let stream = sourceStreamProvider(
    must(ast.table, 'Table not specified in the AST'),
  );

  if (ast.where) {
    stream = applyWhere(stream, ast.where);
  }

  let ret: DifferenceStream<Entity> = stream;
  // groupBy also applied aggregations
  if (ast.groupBy) {
    ret = applyGroupBy(
      ret as DifferenceStream<Entity>,
      ast.groupBy,
      ast.aggregate ?? [],
      Array.isArray(ast.select) ? ast.select : [],
      ast.orderBy,
    ) as unknown as DifferenceStream<Entity>;
  }
  // if there was no group-by then we could be aggregating the entire table
  else if (ast.aggregate) {
    ret = applyFullTableAggregation(
      ret as DifferenceStream<Entity>,
      ast.aggregate,
    );
  }

  // group-by applies the selection set internally.
  if (ast.groupBy === undefined) {
    ret = applySelect(
      ret as DifferenceStream<Entity>,
      ast.select ?? [],
      ast.orderBy,
    );
  }

  // Note: the stream is technically attached at this point.
  // We could detach it until the user actually runs (or subscribes to) the statement as a tiny optimization.
  return ret;
}

export function applySelect(
  stream: DifferenceStream<Entity>,
  select: string[],
  orderBy: Ordering | undefined,
) {
  return stream.map(x => {
    let ret: Record<string, unknown>;
    if (select.length === 0) {
      ret = {...x};
    } else {
      ret = {};
      for (const field of select) {
        ret[field] = (x as Record<string, unknown>)[field];
      }
    }

    addOrdering(ret, x, orderBy);

    return ret;
  }) as unknown as DifferenceStream<Entity>;
}

function addOrdering(
  ret: Record<string, unknown>,
  row: Record<string, unknown>,
  orderBy: Ordering | undefined,
) {
  const orderingValues: unknown[] = [];
  if (orderBy !== undefined) {
    for (const field of orderBy[0]) {
      orderingValues.push(row[field]);
    }
  }

  Object.defineProperty(ret, orderingProp, {
    enumerable: false,
    writable: false,
    configurable: false,
    value: orderingValues,
  });
}

function applyWhere<T extends Entity>(
  stream: DifferenceStream<T>,
  where: Condition,
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
  conditions: Condition[],
) {
  for (const condition of conditions) {
    stream = applyWhere(stream, condition);
  }
  return stream;
}

function applyOr<T extends Entity>(
  stream: DifferenceStream<T>,
  conditions: Condition[],
): DifferenceStream<T> {
  // Or is done by branching the stream and then applying the conditions to each
  // branch. Then we merge the branches back together. At this point we need to
  // ensure we do not get duplicate entries so we add a distinct operator
  const branches = conditions.map(c => applyWhere(stream, c));
  return concat(branches).distinct();
}

function applySimpleCondition<T extends Entity>(
  stream: DifferenceStream<T>,
  condition: SimpleCondition,
) {
  const operator = getOperator(condition);
  const {field} = condition;
  return stream.filter(x => operator((x as Record<string, unknown>)[field]));
}

function applyGroupBy<T extends Entity>(
  stream: DifferenceStream<T>,
  columns: string[],
  aggregations: Aggregation[],
  select: string[],
  orderBy: Ordering | undefined,
) {
  const keyFunction = makeKeyFunction(columns);
  return stream.reduce(
    keyFunction,
    value => value.id as string,
    values => {
      const first = values[Symbol.iterator]().next().value;
      const ret: Record<string, unknown> = {};
      for (const column of select) {
        ret[column] = first[column];
      }
      addOrdering(ret, first, orderBy);

      for (const aggregation of aggregations) {
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
              sum += value[aggregation.field as keyof T] as number;
            }
            ret[aggregation.alias] = sum;
            break;
          }
          case 'avg': {
            let sum = 0;
            let count = 0;
            for (const value of values) {
              sum += value[aggregation.field as keyof T] as number;
              count++;
            }
            ret[aggregation.alias] = sum / count;
            break;
          }
          case 'min': {
            let min = Infinity;
            for (const value of values) {
              min = Math.min(
                min,
                value[aggregation.field as keyof T] as number,
              );
            }
            ret[aggregation.alias] = min;
            break;
          }
          case 'max': {
            let max = -Infinity;
            for (const value of values) {
              max = Math.max(
                max,
                value[aggregation.field as keyof T] as number,
              );
            }
            ret[aggregation.alias] = max;
            break;
          }
          case 'array': {
            ret[aggregation.alias] = Array.from(values).map(
              x => x[aggregation.field as keyof T],
            );
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
        ret = ret.average(agg.field as keyof T, agg.alias);
        break;
      case 'count':
        ret = ret.count(agg.alias);
        break;
      case 'sum':
        ret = ret.sum(agg.field as keyof T, agg.alias);
        break;
    }
  }

  return ret;
}

function makeKeyFunction(columns: string[]) {
  return (x: Record<string, unknown>) => {
    const ret: unknown[] = [];
    for (const column of columns) {
      ret.push(x[column]);
    }
    // Would it be better to come up with some hash function
    // which can handle complex types?
    return JSON.stringify(ret);
  };
}

// We're well-typed in the query builder so once we're down here
// we can assume that the operator is valid.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function getOperator(condition: SimpleCondition): (lhs: any) => boolean {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const rhs = condition.value.value as any;
  const {op} = condition;
  switch (op) {
    case '=':
      return lhs => lhs === rhs;
    case '!=':
      return lhs => lhs !== rhs;
    case '<':
      return lhs => lhs < rhs;
    case '>':
      return lhs => lhs > rhs;
    case '>=':
      return lhs => lhs >= rhs;
    case '<=':
      return lhs => lhs <= rhs;
    case 'IN':
      return lhs => rhs.includes(lhs);
    case 'NOT IN':
      return lhs => !rhs.includes(lhs);
    case 'LIKE':
      return getLikeOp(rhs, '');
    case 'NOT LIKE':
      return not(getLikeOp(rhs, ''));
    case 'ILIKE':
      return getLikeOp(rhs, 'i');
    case 'NOT ILIKE':
      return not(getLikeOp(rhs, 'i'));
    default:
      throw new Error(`Operator ${op} not supported`);
  }
}

function not<T>(f: (lhs: T) => boolean) {
  return (lhs: T) => !f(lhs);
}

function getLikeOp(pattern: string, flags: 'i' | ''): (lhs: string) => boolean {
  // if lhs does not contain '%' or '_' then it is a simple string comparison.
  // if it does contain '%' or '_' then it is a regex comparison.
  // '%' is a wildcard for any number of characters
  // '_' is a wildcard for a single character
  // Postgres SQL allows escaping using `\`.

  if (!/_|%|\\/.test(pattern)) {
    if (flags === 'i') {
      const rhsLower = pattern.toLowerCase();
      return (lhs: string) => lhs.toLowerCase() === rhsLower;
    }
    return (lhs: string) => lhs === pattern;
  }
  const re = patternToRegExp(pattern, flags);
  return (lhs: string) => re.test(lhs);
}

const specialCharsRe = /[$()*+.?[\]\\^{|}]/;

function patternToRegExp(source: string, flags: '' | 'i' = ''): RegExp {
  // There are a few cases:
  // % => .*
  // _ => .
  // \x => \x for any x except special regexp chars
  // special regexp chars => \special regexp chars
  let pattern = '^';
  for (let i = 0; i < source.length; i++) {
    let c = source[i];
    switch (c) {
      case '%':
        pattern += '.*';
        break;
      case '_':
        pattern += '.';
        break;

      // @ts-expect-error fallthrough
      case '\\':
        if (i === source.length - 1) {
          throw new Error('LIKE pattern must not end with escape character');
        }
        i++;
        c = source[i];

      // fall through
      default:
        if (specialCharsRe.test(c)) {
          pattern += '\\';
        }
        pattern += c;

        break;
    }
  }
  return new RegExp(pattern + '$', flags);
}
