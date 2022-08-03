import type {LogContext} from '@rocicorp/logger';
import * as db from '../db/mod';
import type * as dag from '../dag/mod';
import {assertHTTPRequestInfo, HTTPRequestInfo} from '../http-request-info';
import {Pusher, PushError} from '../pusher';
import {callJSRequest} from './js-request';
import {toError} from '../to-error';
import type {InternalValue} from '../internal-value.js';
import {assertLocalMetaDD31} from '../db/commit';

export const PUSH_VERSION = 0;

/**
 * The JSON value used as the body when doing a POST to the [push
 * endpoint](/server-push).
 */
export type PushRequest = {
  profileID: string;
  clientID: string;
  mutations: Mutation[];
  pushVersion: number;
  // schema_version can optionally be used to specify to the push endpoint
  // version information about the mutators the app is using (e.g., format
  // of mutator args).
  schemaVersion: string;
};

/**
 * Mutation describes a single mutation done on the client.
 */
export type Mutation = {
  readonly id: number;
  readonly name: string;
  readonly args: InternalValue;
  readonly timestamp: number;
};

/**
 * The JSON value used as the body when doing a POST to the [push
 * endpoint](/server-push).
 */
export type PushRequestDD31 = {
  profileID: string;
  clientID: string;
  mutations: MutationDD31[];
  pushVersion: number;
  // schema_version can optionally be used to specify to the push endpoint
  // version information about the mutators the app is using (e.g., format
  // of mutator args).
  schemaVersion: string;
};

/**
 * Mutation describes a single mutation done on the client.
 */
export type MutationDD31 = {
  readonly clientID: string;
  readonly id: number;
  readonly name: string;
  readonly args: InternalValue;
  readonly timestamp: number;
};

export function convert(lm: db.LocalMeta): Mutation {
  return {
    id: lm.mutationID,
    name: lm.mutatorName,
    args: lm.mutatorArgsJSON,
    timestamp: lm.timestamp,
  };
}

export function convertDD31(lm: db.LocalMeta): MutationDD31 {
  assertLocalMetaDD31(lm);
  return {
    clientID: lm.clientID,
    id: lm.mutationID,
    name: lm.mutatorName,
    args: lm.mutatorArgsJSON,
    timestamp: lm.timestamp,
  };
}

export async function push(
  requestID: string,
  store: dag.Store,
  lc: LogContext,
  profileID: string,
  clientID: string,
  pusher: Pusher,
  pushURL: string,
  auth: string,
  schemaVersion: string,
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
  if (pending.length > 0) {
    let pushReq: PushRequest | PushRequestDD31;
    if (DD31) {
      const pushMutations: MutationDD31[] = [];
      for (const commit of pending) {
        if (commit.isLocal()) {
          pushMutations.push(convertDD31(commit.meta));
        } else {
          throw new Error('Internal non local pending commit');
        }
      }
      pushReq = {
        profileID,
        clientID,
        mutations: pushMutations,
        pushVersion: PUSH_VERSION,
        schemaVersion,
      };
    } else {
      const pushMutations: Mutation[] = [];
      for (const commit of pending) {
        if (commit.isLocal()) {
          pushMutations.push(convert(commit.meta));
        } else {
          throw new Error('Internal non local pending commit');
        }
      }
      pushReq = {
        profileID,
        clientID,
        mutations: pushMutations,
        pushVersion: PUSH_VERSION,
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
  body: PushRequest | PushRequestDD31,
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
