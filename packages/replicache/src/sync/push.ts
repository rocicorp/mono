import type {LogContext} from '@rocicorp/logger';
import {assert} from 'shared/src/asserts.js';
import {jsonSchema} from 'shared/src/json-schema.js';
import * as valita from 'shared/src/valita.js';
import type {Store} from '../dag/store.js';
import {
  DEFAULT_HEAD_NAME,
  LocalMetaDD31,
  LocalMetaSDD,
  commitIsLocalDD31,
  commitIsLocalSDD,
  localMutations,
} from '../db/commit.js';
import type {FrozenJSONValue, ReadonlyJSONValue} from '../json.js';
import {
  PushError,
  Pusher,
  PusherResult,
  assertPusherResult,
} from '../pusher.js';
import {toError} from '../to-error.js';
import {withRead} from '../with-transactions.js';
import {
  clientGroupIDSchema,
  clientIDSchema,
  type ClientGroupID,
  type ClientID,
} from './ids.js';

export const PUSH_VERSION_SDD = 0;
export const PUSH_VERSION_DD31 = 1;

/**
 * Mutation describes a single mutation done on the client. This is the legacy
 * version (V0) and it is used when recovering mutations from old clients.
 */
export type MutationV0 = {
  readonly id: number;
  readonly name: string;
  readonly args: ReadonlyJSONValue;
  readonly timestamp: number;
};

const mutationV0Schema: valita.Type<MutationV0> = valita.readonlyObject({
  id: valita.number(),
  name: valita.string(),
  args: jsonSchema,
  timestamp: valita.number(),
});

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

const mutationV1Schema: valita.Type<MutationV1> = valita.readonlyObject({
  id: valita.number(),
  name: valita.string(),
  args: jsonSchema,
  timestamp: valita.number(),
  clientID: clientIDSchema,
});

type FrozenMutationV0 = Omit<MutationV0, 'args'> & {
  readonly args: FrozenJSONValue;
};

/**
 * The JSON value used as the body when doing a POST to the [push
 * endpoint](/reference/server-push). This is the legacy version (V0) and it is
 * still used when recovering mutations from old clients.
 */
export type PushRequestV0 = {
  pushVersion: 0;
  /**
   * `schemaVersion` can optionally be used to specify to the push endpoint
   * version information about the mutators the app is using (e.g., format of
   * mutator args).
   */
  schemaVersion: string;
  profileID: string;

  clientID: ClientID;
  mutations: MutationV0[];
};

const pushRequestV0Schema: valita.Type<PushRequestV0> = valita.object({
  pushVersion: valita.literal(0),
  schemaVersion: valita.string(),
  profileID: valita.string(),
  clientID: clientIDSchema,
  mutations: valita.array(mutationV0Schema),
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

export type PushRequest = PushRequestV0 | PushRequestV1;

export function assertPushRequestV0(
  value: unknown,
): asserts value is PushRequestV0 {
  valita.assert(value, pushRequestV0Schema);
}

export function assertPushRequestV1(
  value: unknown,
): asserts value is PushRequestV1 {
  valita.assert(value, pushRequestV1Schema);
}

/**
 * Mutation describes a single mutation done on the client.
 */
type FrozenMutationV1 = FrozenMutationV0 & {
  readonly clientID: ClientID;
};

function convertSDD(lm: LocalMetaSDD): FrozenMutationV0 {
  return {
    id: lm.mutationID,
    name: lm.mutatorName,
    args: lm.mutatorArgsJSON,
    timestamp: lm.timestamp,
  };
}

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
  clientID: ClientID,
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

  let pushReq: PushRequestV0 | PushRequestV1;

  if (pushVersion === PUSH_VERSION_DD31) {
    const pushMutations: FrozenMutationV1[] = [];
    for (const commit of pending) {
      if (commitIsLocalDD31(commit)) {
        pushMutations.push(convertDD31(commit.meta));
      } else {
        throw new Error('Internal non local pending commit');
      }
    }
    assert(clientGroupID);
    const r: PushRequestV1 = {
      profileID,
      clientGroupID,
      mutations: pushMutations,
      pushVersion: PUSH_VERSION_DD31,
      schemaVersion,
    };
    pushReq = r;
  } else {
    assert(pushVersion === PUSH_VERSION_SDD);
    const pushMutations: FrozenMutationV0[] = [];
    for (const commit of pending) {
      if (commitIsLocalSDD(commit)) {
        pushMutations.push(convertSDD(commit.meta));
      } else {
        throw new Error('Internal non local pending commit');
      }
    }
    pushReq = {
      profileID,
      clientID,
      mutations: pushMutations,
      pushVersion: PUSH_VERSION_SDD,
      schemaVersion,
    };
  }
  lc.debug?.('Starting push...');
  const pushStart = Date.now();
  const pusherResult = await callPusher(pusher, pushReq, requestID);
  lc.debug?.('...Push complete in ', Date.now() - pushStart, 'ms');
  return pusherResult;
}

async function callPusher(
  pusher: Pusher,
  body: PushRequestV0 | PushRequestV1,
  requestID: string,
): Promise<PusherResult> {
  try {
    const pusherResult = await pusher(body, requestID);
    assertPusherResult(pusherResult);
    return pusherResult;
  } catch (e) {
    throw new PushError(toError(e));
  }
}
