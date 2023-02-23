import type {LogContext} from '@rocicorp/logger';
import * as db from '../db/mod.js';
import type * as dag from '../dag/mod.js';
import {
  assertPusherResult,
  Pusher,
  PusherResult,
  PushError,
} from '../pusher.js';
import {toError} from '../to-error.js';
import {commitIsLocalDD31, commitIsLocalSDD} from '../db/commit.js';
import type {ClientID, ClientGroupID} from './ids.js';
import {
  assert,
  assertArray,
  assertNumber,
  assertObject,
  assertString,
} from '../asserts.js';
import {
  assertJSONValue,
  FrozenJSONValue,
  ReadonlyJSONObject,
  ReadonlyJSONValue,
} from '../json.js';
import {withRead} from '../with-transactions.js';

export const PUSH_VERSION_SDD = 0;
export const PUSH_VERSION_DD31 = 1;

/**
 * The JSON value used as the body when doing a POST to the [push
 * endpoint](/reference/server-push). This is the legacy version (V0) and it is
 * still used when recovering mutations from old clients.
 */
export type PushRequestSDD = {
  pushVersion: 0;
  /**
   * `schemaVersion` can optionally be used to specify to the push endpoint
   * version information about the mutators the app is using (e.g., format of
   * mutator args).
   */
  schemaVersion: string;
  profileID: string;

  clientID: ClientID;
  mutations: MutationSDD[];
};

/**
 * The JSON value used as the body when doing a POST to the [push
 * endpoint](/reference/server-push).
 */
export type PushRequestDD31 = {
  pushVersion: 1;
  /**
   * `schemaVersion` can optionally be used to specify to the push endpoint
   * version information about the mutators the app is using (e.g., format of
   * mutator args).
   */
  schemaVersion: string;
  profileID: string;

  clientGroupID: ClientGroupID;
  mutations: MutationDD31[];
};

function assertPushRequestBase(v: unknown): asserts v is ReadonlyJSONObject {
  assertObject(v);
  assertString(v.schemaVersion);
  assertString(v.profileID);
}

export function assertPushRequestDD31(
  v: unknown,
): asserts v is PushRequestDD31 {
  assertPushRequestBase(v);
  assertString(v.clientGroupID);
  assertArray(v.mutations);
  v.mutations.forEach(assertMutationsDD31);
}

/**
 * Mutation describes a single mutation done on the client. This is the legacy
 * version (V0) and it is used when recovering mutations from old clients.
 */
export type MutationSDD = {
  readonly id: number;
  readonly name: string;
  readonly args: ReadonlyJSONValue;
  readonly timestamp: number;
};

/**
 * Mutation describes a single mutation done on the client.
 */
export type MutationDD31 = {
  readonly id: number;
  readonly name: string;
  readonly args: ReadonlyJSONValue;
  readonly timestamp: number;
  readonly clientID: ClientID;
};

function assertMutationsSDD(v: unknown): asserts v is MutationSDD {
  assertObject(v);
  assertNumber(v.id);
  assertString(v.name);
  assertJSONValue(v.args);
  assertNumber(v.timestamp);
}

function assertMutationsDD31(v: unknown): asserts v is MutationDD31 {
  assertMutationsSDD(v);
  assertString((v as Partial<MutationDD31>).clientID);
}

type FrozenMutationSDD = Omit<MutationSDD, 'args'> & {
  readonly args: FrozenJSONValue;
};

/**
 * Mutation describes a single mutation done on the client.
 */
type FrozenMutationDD31 = FrozenMutationSDD & {
  readonly clientID: ClientID;
};

function convertSDD(lm: db.LocalMetaSDD): FrozenMutationSDD {
  return {
    id: lm.mutationID,
    name: lm.mutatorName,
    args: lm.mutatorArgsJSON,
    timestamp: lm.timestamp,
  };
}

function convertDD31(lm: db.LocalMetaDD31): FrozenMutationDD31 {
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
  store: dag.Store,
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
    const mainHeadHash = await dagRead.getHead(db.DEFAULT_HEAD_NAME);
    if (!mainHeadHash) {
      throw new Error('Internal no main head');
    }
    return await db.localMutations(mainHeadHash, dagRead);
    // Important! Don't hold the lock through an HTTP request!
  });

  if (pending.length === 0) {
    return undefined;
  }

  // Commit.pending gave us commits in head-first order; the bindings
  // want tail first (in mutation id order).
  pending.reverse();

  let pushReq: PushRequestSDD | PushRequestDD31;

  if (pushVersion === PUSH_VERSION_DD31) {
    const pushMutations: FrozenMutationDD31[] = [];
    for (const commit of pending) {
      if (commitIsLocalDD31(commit)) {
        pushMutations.push(convertDD31(commit.meta));
      } else {
        throw new Error('Internal non local pending commit');
      }
    }
    assert(clientGroupID);
    const r: PushRequestDD31 = {
      profileID,
      clientGroupID,
      mutations: pushMutations,
      pushVersion: PUSH_VERSION_DD31,
      schemaVersion,
    };
    pushReq = r;
  } else {
    assert(pushVersion === PUSH_VERSION_SDD);
    const pushMutations: FrozenMutationSDD[] = [];
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
  body: PushRequestSDD | PushRequestDD31,
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
