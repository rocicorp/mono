import type {Ordering, Selector, SimpleOperator} from '../../ast/ast.js';

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

export function forkPullMessage(original: Request): Request {
  return {
    ...original,
    id: nextMessageID(),
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

// TODO: we can merge and expand inequalities and `INs`
export function intersectConditions(
  a: readonly HoistedCondition[],
  b: readonly HoistedCondition[],
) {
  if (a === b) {
    return a;
  }

  const valueMap = new Map<string, unknown[]>();
  const makeKey = (cond: HoistedCondition) =>
    cond.op + '-' + cond.selector.join(',');

  for (const cond of a) {
    const key = makeKey(cond);
    const existing = valueMap.get(key);
    if (existing) {
      existing.push(cond.value);
    } else {
      valueMap.set(key, [cond.value]);
    }
  }

  const intersection: HoistedCondition[] = [];
  for (const cond of b) {
    const key = makeKey(cond);
    const existing = valueMap.get(key);
    if (existing && existing.find(v => v === cond.value) !== undefined) {
      intersection.push(cond);
    }
  }

  if (intersection.length === a.length) {
    return a;
  }

  return intersection;
}
