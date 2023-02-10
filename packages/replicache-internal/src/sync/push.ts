import type {LogContext} from '@rocicorp/logger';
import * as db from '../db/mod';
import type * as dag from '../dag/mod';
import {assertHTTPRequestInfo, HTTPRequestInfo} from '../http-request-info';
import {Pusher, PushError} from '../pusher';
import {callJSRequest} from './js-request';
import {toError} from '../to-error';
import type {InternalValue} from '../internal-value';
import {commitIsLocalDD31, commitIsLocalSDD} from '../db/commit';
import {assert} from '../asserts.js';
import type {ClientGroupID} from './client-group-id.js';
import type {ClientID} from './ids.js';
import type {ReadonlyJSONValue} from '../json.js';

export const PUSH_VERSION_SDD = 0;
export const PUSH_VERSION_DD31 = 1;

/**
 * The JSON value used as the body when doing a POST to the [push
 * endpoint](/server-push).
 */
export type PushRequest = {
  profileID: string;
  clientID: ClientID;
  mutations: Mutation[];
  pushVersion: number;
  /**
   * `schemaVersion` can optionally be used to specify to the push endpoint
   * version information about the mutators the app is using (e.g., format of
   * mutator args).
   */
  schemaVersion: string;
};

export type PushRequestSDD = Omit<PushRequest, 'mutations' | 'pushVersion'> & {
  mutations: MutationSDD[];
  pushVersion: typeof PUSH_VERSION_SDD;
};

export type PushRequestDD31 = Omit<PushRequest, 'mutations' | 'pushVersion'> & {
  clientGroupID: ClientGroupID;
  mutations: MutationDD31[];
  pushVersion: typeof PUSH_VERSION_DD31;
};

/**
 * Mutation describes a single mutation done on the client.
 */
export type Mutation = {
  readonly id: number;
  readonly name: string;
  readonly args: ReadonlyJSONValue;
  readonly timestamp: number;
};

export type MutationSDD = Omit<Mutation, 'args'> & {
  readonly args: InternalValue;
};

/**
 * Mutation describes a single mutation done on the client.
 */
export type MutationDD31 = MutationSDD & {
  readonly clientID: ClientID;
};

function convertSDD(lm: db.LocalMetaSDD): MutationSDD {
  return {
    id: lm.mutationID,
    name: lm.mutatorName,
    args: lm.mutatorArgsJSON,
    timestamp: lm.timestamp,
  };
}

function convertDD31(lm: db.LocalMetaDD31): MutationDD31 {
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
  pushURL: string,
  auth: string,
  schemaVersion: string,
  pushVersion: typeof PUSH_VERSION_SDD | typeof PUSH_VERSION_DD31,
): Promise<HTTPRequestInfo | undefined> {
  // Find pending commits between the base snapshot and the main head and push
  // them to the data layer.
  const pending = await store.withRead(async dagRead => {
    const mainHeadHash = await dagRead.getHead(db.DEFAULT_HEAD_NAME);
    if (!mainHeadHash) {
      throw new Error('Internal no main head');
    }
    return await db.localMutations(mainHeadHash, dagRead);
    // Important! Don't hold the lock through an HTTP request!
  });
  // Commit.pending gave us commits in head-first order; the bindings
  // want tail first (in mutation id order).
  pending.reverse();

  let httpRequestInfo: HTTPRequestInfo | undefined = undefined;
  let pushReq: PushRequestSDD | PushRequestDD31;

  if (pending.length > 0) {
    if (DD31 && pushVersion === PUSH_VERSION_DD31) {
      const pushMutations: MutationDD31[] = [];
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
        clientID,
        mutations: pushMutations,
        pushVersion: PUSH_VERSION_DD31,
        schemaVersion,
      };
      pushReq = r;
    } else {
      assert(pushVersion === PUSH_VERSION_SDD);
      const pushMutations: MutationSDD[] = [];
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
    httpRequestInfo = await callPusher(
      pusher,
      pushURL,
      pushReq,
      auth,
      requestID,
    );
    lc.debug?.('...Push complete in ', Date.now() - pushStart, 'ms');
  }

  return httpRequestInfo;
}

async function callPusher(
  pusher: Pusher,
  url: string,
  body: PushRequestSDD | PushRequestDD31,
  auth: string,
  requestID: string,
): Promise<HTTPRequestInfo> {
  try {
    const res = await callJSRequest(pusher, url, body, auth, requestID);
    assertHTTPRequestInfo(res);
    return res;
  } catch (e) {
    throw new PushError(toError(e));
  }
}
