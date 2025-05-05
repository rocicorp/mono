import {LogContext} from '@rocicorp/logger';
import type {Socket} from 'node:net';
import UrlPattern from 'url-pattern';
import {assert} from '../../../shared/src/asserts.ts';
import {h32} from '../../../shared/src/hash.ts';
import {getSubscriberContext} from '../services/change-streamer/change-streamer-http.ts';
import {RunningState} from '../services/running-state.ts';
import type {Service} from '../services/service.ts';
import type {IncomingMessageSubset} from '../types/http.ts';
import type {Worker} from '../types/processes.ts';
import {
  createWebSocketHandoffHandler,
  type Handoff,
  type WebSocketHandoffHandler,
} from '../types/websocket-handoff.ts';
import {getConnectParams} from '../workers/connect-params.ts';

export class WorkerDispatcher implements Service {
  readonly id = 'worker-dispatcher';
  readonly #lc: LogContext;

  readonly #state = new RunningState(this.id);

  constructor(
    lc: LogContext,
    taskID: string,
    parent: Worker,
    syncers: Worker[],
    mutator: Worker | undefined,
    changeStreamer: Worker | undefined,
  ) {
    this.#lc = lc;

    function connectParams(req: IncomingMessageSubset) {
      const {headers, url: u} = req;
      const url = new URL(u ?? '', 'http://unused/');
      const path = parsePath(url);
      if (!path) {
        throw new Error(`Invalid URL: ${u}`);
      }
      const version = Number(path.version);
      if (Number.isNaN(version)) {
        throw new Error(`Invalid version: ${u}`);
      }
      const {params, error} = getConnectParams(version, url, headers);
      if (error !== null) {
        throw new Error(error);
      }
      return params;
    }

    const pushHandler = createWebSocketHandoffHandler(lc, req => {
      assert(
        mutator !== undefined,
        'Received a push for a custom mutation but no `push.url` was configured.',
      );
      return {payload: connectParams(req), receiver: mutator};
    });

    const syncHandler = createWebSocketHandoffHandler(lc, req => {
      assert(syncers.length, 'Received a sync request with no sync workers.');
      const params = connectParams(req);
      const {clientGroupID} = params;

      // Include the TaskID when hash-bucketting the client group to the sync
      // worker. This diversifies the distribution of client groups (across
      // workers) for different tasks, so that if one task sheds connections
      // from its most heavily loaded sync worker(s), those client groups will
      // be distributed uniformly across workers on the receiving task(s).
      const syncer = h32(taskID + '/' + clientGroupID) % syncers.length;

      lc.debug?.(`connecting ${clientGroupID} to syncer ${syncer}`);
      return {payload: params, receiver: syncers[syncer]};
    });

    const changeStreamerHandler = createWebSocketHandoffHandler(lc, req => {
      // Note: The change-streamer is generally not dispatched via the main
      //       port, and in particular, should *not* be accessible via that
      //       port in single-node mode. However, this plumbing is maintained
      //       for the purpose of allowing --lazy-startup of the
      //       replication-manager as a possible future feature.
      assert(
        syncers.length === 0 && mutator === undefined,
        'Dispatch to the change-streamer via the main port ' +
          'is only allowed in multi-node mode',
      );
      assert(
        changeStreamer,
        'Received a change-streamer request without a change-streamer worker',
      );
      return {
        payload: getSubscriberContext(req),
        receiver: changeStreamer,
      };
    });

    // handoff messages from this ZeroDispatcher to the appropriate worker (pool).
    parent.onMessageType<Handoff<unknown>>('handoff', (msg, socket) => {
      const {message, head} = msg;
      const {url: u} = message;
      const url = new URL(u ?? '', 'http://unused/');
      const path = parsePath(url);
      if (!path) {
        throw new Error(`Invalid URL: ${u}`);
      }
      const handleWith = (handle: WebSocketHandoffHandler) =>
        handle(message, socket as Socket, Buffer.from(head));
      switch (path.worker) {
        case 'sync':
          return handleWith(syncHandler);
        case 'replication':
          return handleWith(changeStreamerHandler);
        case 'mutate':
          return handleWith(pushHandler);
        default:
          throw new Error(`Invalid URL: ${u}`);
      }
    });
  }

  run() {
    return this.#state.stopped();
  }

  stop() {
    this.#state.stop(this.#lc);
    return this.#state.stopped();
  }
}

export const URL_PATTERN = new UrlPattern('(/:base)/:worker/v:version/:action');

export function parsePath(
  url: URL,
):
  | {base?: string; worker: 'sync' | 'mutate' | 'replication'; version: string}
  | undefined {
  // The match() returns both null and undefined.
  return URL_PATTERN.match(url.pathname) || undefined;
} // The server allows the client to use any /:base/ path to facilitate
// servicing requests on the same domain as the application.
