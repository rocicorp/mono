import type {Ordering, Selector, SimpleOperator} from '../../ast/ast.js';
import {compareEntityFields} from '../compare.js';

export type Request = PullMsg;

/**
 * Used to pull historical data down the pipeline.
 *
 * Sources of history may not need to send _all_ history.
 *
 * To deal with that, the graph collects information
 * about what could constrain the data to send.
 *
 * The view sets:
 * - ordering
 * - query type
 *
 * Upstream operators set:
 * - hoistedConditions
 *
 * Querying historical data vs responding to changes in
 * data are slightly different problems.
 *
 * E.g.,
 *
 * "Find me all items in the set greater than Y" ->
 * SELECT * FROM set WHERE item > Y
 *
 *
 * vs
 *
 * "Find me all queries that do not care about the value of Y
 *  or where Y is less then item"
 *
 * Pulling answers the former. The data flow graph
 * answers the latter.
 */
export type PullMsg = {
  readonly id: number;
  readonly type: 'pull';
  // undefined means that the data does not need to be ordered in the reply.
  readonly order?: Ordering | undefined;
  readonly hoistedConditions: readonly HoistedCondition[];
};
export type HoistedCondition = {
  selector: readonly [string | null, string];
  op: SimpleOperator;
  value: unknown;
};

export type Reply = PullReplyMsg;

export type PullReplyMsg = {
  readonly replyingTo: number;
  readonly type: 'pullResponse';
  readonly sourceName: string;
  // the order of the data we are sending.
  // undefined means that it is not ordered.
  readonly order: Ordering | undefined;
  // columns we are not ordered by but do produce contiguous groupings for.
  // E.g., `SELECT * FROM a LEFT JOIN b ON a.id = b.a_id ORDER BY a.modified`
  // We sort on `a.modified` but we also have a contiguous group on `a.id`.
  // As in, `a.id` is unique and is only duplicated by the join which duplicates the row
  // in a contiguous group.
  readonly contiguousGroup: readonly Selector[];
};

let messageID = 0;

function nextMessageID() {
  return messageID++;
}

/**
 * PullMessage is sent by leaves up to sources to tell them to send
 * historical data.
 *
 * In the future, pull messages will gather up hoistable
 * expressions and send them to the source to be evaluated.
 *
 * E.g., if there is a filter against the primary key. The source
 * can use that information to restrict the rows it returns.
 */
export function createPullMessage(order: Ordering | undefined): Request {
  return {
    id: nextMessageID(),
    type: 'pull',
    order,
    hoistedConditions: [],
  };
}

export function createPullResponseMessage(
  pullMsg: PullMsg,
  sourceName: string,
  order: Ordering | undefined,
): Reply {
  return {
    replyingTo: pullMsg.id,
    type: 'pullResponse',
    sourceName,
    order,
    contiguousGroup: [],
  };
}

export function mergeConditionLists(
  a: readonly HoistedCondition[],
  b: readonly HoistedCondition[],
) {
  if (a === b) {
    return a;
  }

  const aConditionsBySelector = groupConditionsBySelector(a);
  const bConditionsBySelector = groupConditionsBySelector(b);
  const ret: HoistedCondition[] = [];

  for (const [selector, aConditions] of aConditionsBySelector) {
    const bConditions = bConditionsBySelector.get(selector);
    if (bConditions) {
      for (const aCondition of aConditions) {
        const merged = mergeConditions(aCondition, bConditions);
        if (merged) {
          ret.push(merged);
        }
      }
    }
    // no corresponding b condition? throw it out.
  }

  return ret;
}

function groupConditionsBySelector(
  conditions: readonly HoistedCondition[],
): Map<string, HoistedCondition[]> {
  const ret = new Map<string, HoistedCondition[]>();

  for (const condition of conditions) {
    const key = condition.selector.join(',');
    const existing = ret.get(key);
    if (existing) {
      existing.push(condition);
    } else {
      ret.set(key, [condition]);
    }
  }

  return ret;
}

export function mergeConditions(
  left: HoistedCondition,
  right: HoistedCondition[],
): HoistedCondition | undefined {
  if (right.length === 0) {
    return undefined;
  }
  if (right.length === 1) {
    if (left === right[0]) {
      return left;
    }
    if (left.op === right[0].op && left.value === right[0].value) {
      return left;
    }
  }

  const ret = {
    ...left,
  };

  for (const cond of right) {
    switch (ret.op) {
      case '<': {
        switch (cond.op) {
          case '<':
            ret.value = max(ret.value, cond.value);
            break;
          case '<=':
            if (compareEntityFields(ret.value, cond.value) <= 0) {
              ret.value = cond.value;
              ret.op = cond.op;
            }
            break;
          case '=':
            if (compareEntityFields(ret.value, cond.value) <= 0) {
              ret.value = cond.value;
              ret.op = '<=';
            }
            break;
          default:
            return undefined;
        }
        break;
      }
      case '<=': {
        switch (cond.op) {
          case '<':
            if (compareEntityFields(ret.value, cond.value) < 0) {
              ret.value = cond.value;
              ret.op = cond.op;
            }
            break;
          case '<=':
            ret.value = max(ret.value, cond.value);
            break;
          case '=':
            if (compareEntityFields(ret.value, cond.value) <= 0) {
              ret.value = cond.value;
            }
            break;
          default:
            return undefined;
        }
        break;
      }
      case '=':
        switch (cond.op) {
          case '<':
            if (compareEntityFields(ret.value, cond.value) < 0) {
              ret.value = cond.value;
              ret.op = cond.op;
            } else {
              ret.op = '<=';
            }
            break;
          case '<=':
            if (compareEntityFields(ret.value, cond.value) < 0) {
              ret.value = cond.value;
              ret.op = cond.op;
            } else {
              ret.op = '<=';
            }
            break;
          case '>':
            if (compareEntityFields(ret.value, cond.value) < 0) {
              ret.op = '>=';
            } else {
              ret.op = '>';
              ret.value = cond.value;
            }
            break;
          case '>=':
            if (compareEntityFields(ret.value, cond.value) < 0) {
              ret.op = '>=';
            } else {
              ret.op = '>=';
              ret.value = cond.value;
            }
            break;
          default:
            return undefined;
        }
        break;
      case '>':
        switch (cond.op) {
          case '>':
            ret.value = min(ret.value, cond.value);
            break;
          case '>=':
            if (compareEntityFields(ret.value, cond.value) >= 0) {
              ret.value = cond.value;
              ret.op = cond.op;
            }
            break;
          case '=':
            if (compareEntityFields(ret.value, cond.value) >= 0) {
              ret.value = cond.value;
              ret.op = '>=';
            }
            break;
          default:
            return undefined;
        }
        break;
      case '>=':
        switch (cond.op) {
          case '>':
            if (compareEntityFields(ret.value, cond.value) > 0) {
              ret.value = cond.value;
              ret.op = cond.op;
            }
            break;
          case '>=':
            ret.value = min(ret.value, cond.value);
            break;
          case '=':
            if (compareEntityFields(ret.value, cond.value) > 0) {
              ret.value = cond.value;
              ret.op = '>=';
            }
            break;
        }
        break;
      default:
        // TODO: like expansion, in expansion, set operation expansion
        // TODO: > x & < y -> between x and y if y > x
        // TODO: many equality -> IN
        return undefined;
    }
  }

  return ret;
}

function min(l: unknown, r: unknown) {
  const comp = compareEntityFields(l, r);
  if (comp <= 0) {
    return l;
  }
  return r;
}

function max(l: unknown, r: unknown) {
  const comp = compareEntityFields(l, r);
  if (comp < 0) {
    return r;
  }
  return l;
}
