import type {LogContext} from '@rocicorp/logger';
import * as db from '../db/mod';
import type * as dag from '../dag/mod';
import {assertHTTPRequestInfo} from '../http-request-info';
import {Pusher, PusherResult, PushError} from '../pusher';
import {callJSRequest} from './js-request';
import {toError} from '../to-error';
import type {InternalValue} from '../internal-value.js';
import {
  ClientStateNotFoundResponse,
  isClientStateNotFoundResponse,
} from './errors';

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
 * The response to a push requests can either be completely empty, or JSON
 * with the application/json content type. If the latter, these are the
 * supported shapes.
 */
export type PushResponse = PushResponseOK | ClientStateNotFoundResponse;

export type PushResponseOK = Record<string, never>;

export function assertPushResponse(v: unknown): asserts v is PushResponse {
  if (typeof v !== 'object' || v === null) {
    throw new Error('PushResponse must be an object');
  }
  if (isClientStateNotFoundResponse(v)) {
    return;
  }
  // TODO: If push ever has fields in the success case, validiating them goes
  // here.
  return;
}

/**
 * Mutation describes a single mutation done on the client.
 */
export type Mutation = {
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
): Promise<PusherResult | undefined> {
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

  let result: PusherResult | undefined = undefined;
  if (pending.length > 0) {
    const pushMutations: Mutation[] = [];
    for (const commit of pending) {
      if (commit.isLocal()) {
        pushMutations.push(convert(commit.meta));
      } else {
        throw new Error('Internal non local pending commit');
      }
    }
    const pushReq = {
      profileID,
      clientID,
      mutations: pushMutations,
      pushVersion: PUSH_VERSION,
      schemaVersion,
    };
    lc.debug?.('Starting push...');
    const pushStart = Date.now();
    result = await callPusher(pusher, pushURL, pushReq, auth, requestID);
    lc.debug?.('...Push complete in ', Date.now() - pushStart, 'ms');
  }

  return result;
}

async function callPusher(
  pusher: Pusher,
  url: string,
  body: PushRequest,
  auth: string,
  requestID: string,
): Promise<PusherResult> {
  try {
    let res = await callJSRequest(pusher, url, body, auth, requestID);
    if (!('httpRequestInfo' in res)) {
      res = {
        httpRequestInfo: res,
      };
    }
    assertResult(res);
    return res;
  } catch (e) {
    throw new PushError(toError(e));
  }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function assertResult(v: any): asserts v is PusherResult {
  if (typeof v !== 'object' || v === null) {
    throw new Error('Expected result to be an object');
  }

  if (v.response !== undefined) {
    assertPushResponse(v.response);
  }

  assertHTTPRequestInfo(v.httpRequestInfo);
}
