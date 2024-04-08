import {Ordering, SimpleCondition} from '../../ast/ast.js';

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
  readonly order?: Ordering | undefined;
  readonly hoistedConditions: SimpleCondition[];
  readonly queryType?: 'count' | 'select' | undefined;
};

export type Reply = PullReplyMsg;

export type PullReplyMsg = {
  readonly replyingTo: number;
  readonly type: 'pullResponse';
};

let messageID = 0;

export function nextMessageID() {
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
export function createPullMessage(
  order: Ordering | undefined,
  queryType?: 'count' | 'select' | undefined,
): Request {
  return {
    id: nextMessageID(),
    type: 'pull',
    order,
    hoistedConditions: [],
    queryType,
  };
}

export function createPullResponseMessage(pullMsg: PullMsg): Reply {
  return {
    replyingTo: pullMsg.id,
    type: 'pullResponse',
  };
}
