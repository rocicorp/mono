import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../shared/src/asserts.ts';
import {jsonSchema} from '../../../shared/src/json-schema.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import * as valita from '../../../shared/src/valita.ts';
import type {Store} from '../dag/store.ts';
import {
  DEFAULT_HEAD_NAME,
  type LocalMetaDD31,
  commitIsLocalDD31,
  localMutations,
} from '../db/commit.ts';
import type {FrozenJSONValue} from '../frozen-json.ts';
import {
  PushError,
  type Pusher,
  type PusherResult,
  assertPusherResult,
} from '../pusher.ts';
import {ReportError} from '../replicache.ts';
import {toError} from '../to-error.ts';
import {withRead} from '../with-transactions.ts';
import {
  type ClientGroupID,
  type ClientID,
  clientGroupIDSchema,
  clientIDSchema,
} from './ids.ts';

export const PUSH_VERSION_SDD = 0;
export const PUSH_VERSION_DD31 = 1;

/**
 * Mutation describes a single mutation done on the client.
 */
export type MutationV1 = {
  readonly id: number;
  readonly name: string;
  readonly args: ReadonlyJSONValue;
  readonly timestamp: number;
  readonly clientID: ClientID;
};

export type Mutation = MutationV1;

const mutationV1Schema: valita.Type<MutationV1> = valita.readonlyObject({
  id: valita.number(),
  name: valita.string(),
  args: jsonSchema,
  timestamp: valita.number(),
  clientID: clientIDSchema,
});

/**
 * The JSON value used as the body when doing a POST to the [push
 * endpoint](/reference/server-push).
 */
export type PushRequestV1 = {
  pushVersion: 1;
  /**
   * `schemaVersion` can optionally be used to specify to the push endpoint
   * version information about the mutators the app is using (e.g., format of
   * mutator args).
   */
  schemaVersion: string;
  profileID: string;

  clientGroupID: ClientGroupID;
  mutations: MutationV1[];
};

const pushRequestV1Schema = valita.object({
  pushVersion: valita.literal(1),
  schemaVersion: valita.string(),
  profileID: valita.string(),
  clientGroupID: clientGroupIDSchema,
  mutations: valita.array(mutationV1Schema),
});

export type PushRequest = PushRequestV1;

export function assertPushRequestV1(
  value: unknown,
): asserts value is PushRequestV1 {
  valita.assert(value, pushRequestV1Schema);
}

/**
 * Mutation describes a single mutation done on the client.
 */
type FrozenMutationV1 = {
  readonly id: number;
  readonly name: string;
  readonly args: FrozenJSONValue;
  readonly timestamp: number;
  readonly clientID: ClientID;
};

function convertDD31(lm: LocalMetaDD31): FrozenMutationV1 {
  return {
    id: lm.mutationID,
    name: lm.mutatorName,
    args: lm.mutatorArgsJSON,
    timestamp: lm.timestamp,
    clientID: lm.clientID,
  };
}

export async function push(
  requestID: string,
  store: Store,
  lc: LogContext,
  profileID: string,
  clientGroupID: ClientGroupID | undefined,
  _clientID: ClientID,
  pusher: Pusher,
  schemaVersion: string,
  pushVersion: typeof PUSH_VERSION_SDD | typeof PUSH_VERSION_DD31,
): Promise<PusherResult | undefined> {
  // Find pending commits between the base snapshot and the main head and push
  // them to the data layer.
  const pending = await withRead(store, async dagRead => {
    const mainHeadHash = await dagRead.getHead(DEFAULT_HEAD_NAME);
    if (!mainHeadHash) {
      throw new Error('Internal no main head');
    }
    return localMutations(mainHeadHash, dagRead);
    // Important! Don't hold the lock through an HTTP request!
  });

  if (pending.length === 0) {
    return undefined;
  }

  // Commit.pending gave us commits in head-first order; the bindings
  // want tail first (in mutation id order).
  pending.reverse();

  assert(pushVersion === PUSH_VERSION_DD31);

  const pushMutations: FrozenMutationV1[] = [];
  for (const commit of pending) {
    if (commitIsLocalDD31(commit)) {
      pushMutations.push(convertDD31(commit.meta));
    } else {
      throw new Error('Internal non local pending commit');
    }
  }
  assert(clientGroupID);
  const pushReq: PushRequestV1 = {
    profileID,
    clientGroupID,
    mutations: pushMutations,
    pushVersion: PUSH_VERSION_DD31,
    schemaVersion,
  };

  lc.debug?.('Starting push...');
  const pushStart = Date.now();
  const pusherResult = await callPusher(pusher, pushReq, requestID);
  lc.debug?.('...Push complete in ', Date.now() - pushStart, 'ms');
  return pusherResult;
}

async function callPusher(
  pusher: Pusher,
  body: PushRequestV1,
  requestID: string,
): Promise<PusherResult> {
  let pusherResult: PusherResult;
  try {
    pusherResult = await pusher(body, requestID);
  } catch (e) {
    throw new PushError(toError(e));
  }
  try {
    assertPusherResult(pusherResult);
    return pusherResult;
  } catch (e) {
    throw new ReportError('Invalid pusher result', toError(e));
  }
}
