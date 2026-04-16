import {ROOT_CONTEXT, context, propagation} from '@opentelemetry/api';
import type {LogContext} from '@rocicorp/logger';
import {groupBy} from '../../../../shared/src/arrays.ts';
import {assert} from '../../../../shared/src/asserts.ts';
import {getErrorMessage} from '../../../../shared/src/error.ts';
import {must} from '../../../../shared/src/must.ts';
import {Queue} from '../../../../shared/src/queue.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {ErrorKind} from '../../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../../../zero-protocol/src/error-reason.ts';
import {
  isProtocolError,
  type PushFailedBody,
} from '../../../../zero-protocol/src/error.ts';
import * as MutationType from '../../../../zero-protocol/src/mutation-type-enum.ts';
import {
  apiMutateResponseSchema,
  CLEANUP_RESULTS_MUTATION_NAME,
  type MutationID,
  type APIMutateResponse,
  type PushBody,
} from '../../../../zero-protocol/src/push.ts';
import {authEquals, isAuthErrorBody} from '../../auth/auth.ts';
import {type ZeroConfig} from '../../config/zero-config.ts';
import {fetchFromAPIServer} from '../../custom/fetch.ts';
import {getOrCreateCounter} from '../../observability/metrics.ts';
import {recordMutation} from '../../server/anonymous-otel-start.ts';
import {ProtocolErrorWithLevel} from '../../types/error-with-level.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import type {HandlerResult, StreamResult} from '../../workers/connection.ts';
import type {RefCountedService, Service} from '../service.ts';
import type {
  ConnectionContext,
  ConnectionContextManager,
  ConnectionSelector,
} from '../view-syncer/connection-context-manager.ts';

export interface Pusher extends RefCountedService {
  initConnection(selector: ConnectionSelector): Source<Downstream>;
  enqueuePush(selector: ConnectionSelector, push: PushBody): HandlerResult;
  ackMutationResponses(
    requester: ConnectionSelector,
    upToID: MutationID,
  ): Promise<void>;
  deleteClientMutations(
    requester: ConnectionSelector,
    clientIDs: string[],
  ): Promise<void>;
}

type Config = Pick<ZeroConfig, 'app' | 'shard'>;

/**
 * Receives push messages from zero-client and forwards
 * them the the user's API server.
 *
 * If the user's API server is taking too long to process
 * the push, the PusherService will add the push to a queue
 * and send pushes in bulk the next time the user's API server
 * is available.
 *
 * - One PusherService exists per client group.
 * - Mutations for a given client are always sent in-order
 * - Mutations for different clients in the same group may be interleaved
 */
export class PusherService implements Service, Pusher {
  readonly id: string;
  readonly #contextManager: ConnectionContextManager;
  readonly #pusher: PushWorker;
  readonly #queue: Queue<PusherEntryOrStop>;
  readonly #config: Config;
  readonly #lc: LogContext;
  #stopped: Promise<void> | undefined;
  #refCount = 0;
  #isStopped = false;

  constructor(
    appConfig: Config,
    lc: LogContext,
    clientGroupID: string,
    contextManager: ConnectionContextManager,
  ) {
    this.#contextManager = contextManager;
    this.#config = appConfig;
    this.#lc = lc.withContext('component', 'pusherService');
    this.#queue = new Queue();
    this.#pusher = new PushWorker(
      appConfig,
      lc,
      this.#contextManager,
      this.#queue,
    );
    this.id = clientGroupID;
  }

  initConnection(selector: ConnectionSelector) {
    return this.#pusher.initConnection(selector);
  }

  enqueuePush(
    selector: ConnectionSelector,
    push: PushBody,
  ): Exclude<HandlerResult, StreamResult> {
    this.#pusher.enqueuePush(
      this.#contextManager.mustGetConnectionContext(selector),
      push,
    );

    return {
      type: 'ok',
    };
  }

  async ackMutationResponses(
    requester: ConnectionSelector,
    upToID: MutationID,
  ): Promise<void> {
    const ctx = this.#contextManager.getConnectionContext(requester);
    if (!ctx?.pushContext?.url) {
      // No push URL configured, skip cleanup
      return;
    }

    const cleanupBody: PushBody = {
      clientGroupID: this.id,
      mutations: [
        {
          type: MutationType.Custom,
          id: 0, // Not tracked - this is fire-and-forget
          clientID: upToID.clientID,
          name: CLEANUP_RESULTS_MUTATION_NAME,
          args: [
            {
              type: 'single',
              clientGroupID: this.id,
              clientID: upToID.clientID,
              upToMutationID: upToID.id,
            },
          ],
          timestamp: Date.now(),
        },
      ],
      pushVersion: 1,
      timestamp: Date.now(),
      requestID: `cleanup-${this.id}-${upToID.clientID}-${upToID.id}`,
    };

    try {
      await fetchFromAPIServer(
        apiMutateResponseSchema,
        'push',
        this.#lc,
        ctx,
        {appID: this.#config.app.id, shardNum: this.#config.shard.num},
        cleanupBody,
      );
    } catch (e) {
      this.#lc.warn?.('Failed to send cleanup mutation', {
        error: getErrorMessage(e),
      });
    }
  }

  /**
   * Bulk cleanup is routed through the requester's push context.
   *
   * This assumes the client group shares a compatible push endpoint/auth
   * context.
   */
  async deleteClientMutations(
    requester: ConnectionSelector,
    clientIDs: string[],
  ): Promise<void> {
    if (clientIDs.length === 0) {
      return;
    }

    const ctx = this.#contextManager.getConnectionContext(requester);
    if (!ctx?.pushContext?.url) {
      // No push URL configured, skip cleanup
      return;
    }

    const cleanupBody: PushBody = {
      clientGroupID: this.id,
      mutations: [
        {
          type: MutationType.Custom,
          id: 0, // Not tracked - this is fire-and-forget
          clientID: clientIDs[0], // Use first client as sender
          name: CLEANUP_RESULTS_MUTATION_NAME,
          args: [
            {
              type: 'bulk',
              clientGroupID: this.id,
              clientIDs,
            },
          ],
          timestamp: Date.now(),
        },
      ],
      pushVersion: 1,
      timestamp: Date.now(),
      requestID: `cleanup-bulk-${this.id}-${Date.now()}`,
    };

    try {
      await fetchFromAPIServer(
        apiMutateResponseSchema,
        'push',
        this.#lc,
        ctx,
        {appID: this.#config.app.id, shardNum: this.#config.shard.num},
        cleanupBody,
      );
    } catch (e) {
      this.#lc.warn?.('Failed to send bulk cleanup mutation', {
        error: getErrorMessage(e),
      });
    }
  }

  ref() {
    assert(!this.#isStopped, 'PusherService is already stopped');
    ++this.#refCount;
  }

  unref() {
    assert(!this.#isStopped, 'PusherService is already stopped');
    --this.#refCount;
    if (this.#refCount <= 0) {
      void this.stop();
    }
  }

  hasRefs(): boolean {
    return this.#refCount > 0;
  }

  run(): Promise<void> {
    this.#stopped = this.#pusher.run();
    return this.#stopped;
  }

  stop(): Promise<void> {
    if (this.#isStopped) {
      return must(this.#stopped, 'Stop was called before `run`');
    }
    this.#isStopped = true;
    this.#queue.enqueue('stop');
    return must(this.#stopped, 'Stop was called before `run`');
  }
}

type PusherEntry = {
  push: PushBody;
  context: ConnectionContext;
};
type PusherEntryOrStop = PusherEntry | 'stop';

/**
 * Awaits items in the queue then drains and sends them all
 * to the user's API server.
 */
class PushWorker {
  readonly #contextManager: ConnectionContextManager;
  readonly #queue: Queue<PusherEntryOrStop>;
  readonly #lc: LogContext;
  readonly #config: Config;
  readonly #clients: Map<
    string,
    {wsID: string; downstream: Subscription<Downstream>}
  >;

  readonly #customMutations = getOrCreateCounter(
    'mutation',
    'custom',
    'Number of custom mutations processed',
  );
  readonly #pushes = getOrCreateCounter(
    'mutation',
    'pushes',
    'Number of pushes processed by the pusher',
  );

  constructor(
    config: Config,
    lc: LogContext,
    contextManager: ConnectionContextManager,
    queue: Queue<PusherEntryOrStop>,
  ) {
    this.#lc = lc.withContext('component', 'pusher');
    this.#contextManager = contextManager;
    this.#queue = queue;
    this.#config = config;
    this.#clients = new Map();
  }

  /**
   * Returns a new downstream stream if the clientID,wsID pair has not been seen before.
   * If a clientID already exists with a different wsID, that client's downstream is cancelled.
   */
  initConnection(selector: ConnectionSelector) {
    const existing = this.#clients.get(selector.clientID);
    if (existing && existing.wsID === selector.wsID) {
      // already initialized for this socket
      throw new Error('Connection was already initialized');
    }

    // client is back on a new connection
    if (existing) {
      existing.downstream.cancel();
    }

    const downstream = Subscription.create<Downstream>({
      cleanup: () => {
        this.#clients.delete(selector.clientID);
      },
    });
    this.#clients.set(selector.clientID, {
      wsID: selector.wsID,
      downstream,
    });
    return downstream;
  }

  enqueuePush(context: ConnectionContext, push: PushBody) {
    this.#queue.enqueue({
      push,
      context,
    });
  }

  async run() {
    for (;;) {
      const task = await this.#queue.dequeue();
      const rest = this.#queue.drain();
      const [pushes, terminate] = combinePushes([task, ...rest]);
      for (const push of pushes) {
        const parentContext = push.push.traceparent
          ? propagation.extract(ROOT_CONTEXT, {
              traceparent: push.push.traceparent,
            })
          : context.active();
        const response = await context.with(parentContext, () =>
          this.#processPush(push),
        );
        await this.#fanOutResponses(response);
      }

      if (terminate) {
        break;
      }
    }
  }

  /**
   * 1. If the entire `push` fails, we send the error to relevant clients.
   * 2. If the push succeeds, we look for any mutation failure that should cause the connection to terminate
   *  and terminate the connection for those clients.
   */
  #fanOutResponses(response: APIMutateResponse) {
    const connectionTerminations: (() => void)[] = [];

    // if the entire push failed, send that to the client.
    if (
      ('kind' in response && response.kind === ErrorKind.PushFailed) ||
      'error' in response
    ) {
      this.#lc.warn?.(
        'The server behind ZERO_MUTATE_URL returned a push error.',
        response,
      );
      // TODO(0xcadams): Fanout is keyed only by clientID here. If a response arrives
      // after reconnect or re-auth, `#clients.get(clientID)` may point at a
      // newer wsID/revision and fail the replacement downstream instead.
      const groupedMutationIDs = groupBy(
        response.mutationIDs ?? [],
        m => m.clientID,
      );
      for (const [clientID, mutationIDs] of groupedMutationIDs) {
        const client = this.#clients.get(clientID);
        if (!client) {
          continue;
        }

        // We do not resolve mutations on the client if the push fails
        // as those mutations will be retried.
        if ('error' in response) {
          // This error code path will eventually be removed when we
          // no longer support the legacy push error format.
          const pushFailedBody: PushFailedBody =
            response.error === 'http'
              ? {
                  kind: ErrorKind.PushFailed,
                  origin: ErrorOrigin.ZeroCache,
                  reason: ErrorReason.HTTP,
                  status: response.status,
                  bodyPreview: response.details,
                  mutationIDs,
                  message: `Fetch from API server returned non-OK status ${response.status}`,
                }
              : response.error === 'unsupportedPushVersion'
                ? {
                    kind: ErrorKind.PushFailed,
                    origin: ErrorOrigin.Server,
                    reason: ErrorReason.UnsupportedPushVersion,
                    mutationIDs,
                    message: `Unsupported push version`,
                  }
                : {
                    kind: ErrorKind.PushFailed,
                    origin: ErrorOrigin.Server,
                    reason: ErrorReason.Internal,
                    mutationIDs,
                    message:
                      response.error === 'zeroPusher'
                        ? response.details
                        : response.error === 'unsupportedSchemaVersion'
                          ? 'Unsupported schema version'
                          : 'An unknown error occurred while pushing to the API server',
                  };

          this.#failDownstream(client.downstream, pushFailedBody);
        } else {
          this.#failDownstream(client.downstream, response);
        }
      }
    } else {
      // Look for mutations results that should cause us to terminate the connection
      // TODO(0xcadams): Same stale-routing issue as above: fatal mutation results are
      // still mapped to the current downstream by clientID only.
      const groupedMutations = groupBy(response.mutations, m => m.id.clientID);
      for (const [clientID, mutations] of groupedMutations) {
        const client = this.#clients.get(clientID);
        if (!client) {
          continue;
        }

        let failure: PushFailedBody | undefined;
        let i = 0;
        for (; i < mutations.length; i++) {
          const m = mutations[i];
          if ('error' in m.result) {
            this.#lc.warn?.(
              'The server behind ZERO_MUTATE_URL returned a mutation error.',
              m.result,
            );
          }
          // This error code path will eventually be removed,
          // keeping this for backwards compatibility, but the server
          // should now return a PushFailedBody with the mutationIDs
          if ('error' in m.result && m.result.error === 'oooMutation') {
            failure = {
              kind: ErrorKind.PushFailed,
              origin: ErrorOrigin.Server,
              reason: ErrorReason.OutOfOrderMutation,
              message: 'mutation was out of order',
              details: m.result.details,
              mutationIDs: mutations.map(m => ({
                clientID: m.id.clientID,
                id: m.id.id,
              })),
            };
            break;
          }
        }

        if (failure && i < mutations.length - 1) {
          this.#lc.warn?.(
            'push-response contains mutations after a mutation which should fatal the connection',
          );
        }

        if (failure) {
          connectionTerminations.push(() =>
            this.#failDownstream(client.downstream, failure),
          );
        }
      }
    }

    connectionTerminations.forEach(cb => cb());
  }

  async #processPush(entry: PusherEntry): Promise<APIMutateResponse> {
    this.#customMutations.add(entry.push.mutations.length, {
      clientGroupID: entry.push.clientGroupID,
    });
    this.#pushes.add(1, {
      clientGroupID: entry.push.clientGroupID,
    });

    // Record custom mutations for telemetry
    recordMutation('custom', entry.push.mutations.length);

    const url = must(
      entry.context.pushContext.url,
      'ZERO_MUTATE_URL is not set',
    );

    this.#lc.debug?.(
      'pushing to',
      url,
      'with',
      entry.push.mutations.length,
      'mutations',
    );

    let mutationIDs: MutationID[] = [];

    try {
      mutationIDs = entry.push.mutations.map(m => ({
        id: m.id,
        clientID: m.clientID,
      }));

      const response = await fetchFromAPIServer(
        apiMutateResponseSchema,
        'push',
        this.#lc,
        entry.context,
        {
          appID: this.#config.app.id,
          shardNum: this.#config.shard.num,
        },
        entry.push,
      );
      if (
        ('kind' in response && response.kind === ErrorKind.PushFailed) ||
        'error' in response
      ) {
        if (isAuthErrorBody(response)) {
          this.#lc.warn?.('Push auth failed; invalidating connection', {
            clientID: entry.context.clientID,
            response: 'kind' in response ? response.message : undefined,
          });
          this.#contextManager.failConnection(
            entry.context,
            entry.context.revision,
          );
        }
        return response;
      }
      // A successful push also validates this connection's current auth snapshot.
      // That lets later shared work reuse it without trusting stale credentials.
      this.#contextManager.validateConnection(
        entry.context,
        entry.context.revision,
      );
      return response;
    } catch (e) {
      if (isProtocolError(e) && e.errorBody.kind === ErrorKind.PushFailed) {
        const response = {
          ...e.errorBody,
          mutationIDs,
        } as const satisfies PushFailedBody;
        if (isAuthErrorBody(response)) {
          this.#lc.warn?.('Push auth failed; invalidating connection', {
            clientID: entry.context.clientID,
            response: 'kind' in response ? response.message : undefined,
          });
          this.#contextManager.failConnection(
            entry.context,
            entry.context.revision,
          );
        }
        return response;
      }

      return {
        kind: ErrorKind.PushFailed,
        origin: ErrorOrigin.ZeroCache,
        reason: ErrorReason.Internal,
        message: `Failed to push: ${getErrorMessage(e)}`,
        mutationIDs,
      } as const satisfies PushFailedBody;
    }
  }

  #failDownstream(
    downstream: Subscription<Downstream>,
    errorBody: PushFailedBody,
  ): void {
    downstream.fail(new ProtocolErrorWithLevel(errorBody, 'warn'));
  }
}

/**
 * Pushes for different clients, sockets, or auth revisions could be interleaved.
 *
 * In order to batch safely, we only combine pushes from the same
 * clientID/wsID/revision snapshot.
 */
export function combinePushes(
  entries: readonly (PusherEntryOrStop | undefined)[],
): [PusherEntry[], boolean] {
  const pushesByConnection = new Map<string, PusherEntry[]>();

  function collect() {
    const ret: PusherEntry[] = [];
    for (const entries of pushesByConnection.values()) {
      const composite: PusherEntry = {
        ...entries[0],
        push: {
          ...entries[0].push,
          mutations: [],
        },
      };
      ret.push(composite);
      for (const entry of entries) {
        assertAreCompatiblePushes(composite, entry);
        composite.push.mutations.push(...entry.push.mutations);
      }
    }
    return ret;
  }

  for (const entry of entries) {
    if (entry === 'stop' || entry === undefined) {
      return [collect(), true];
    }

    const key = `${entry.context.clientID}:${entry.context.wsID}:${entry.context.revision}`;
    const existing = pushesByConnection.get(key);
    if (existing) {
      existing.push(entry);
    } else {
      pushesByConnection.set(key, [entry]);
    }
  }

  return [collect(), false] as const;
}

// These invariants should always be true for a given clientID.
// If they are not, we have a bug in the code somewhere.
function assertAreCompatiblePushes(left: PusherEntry, right: PusherEntry) {
  assert(
    left.context.clientID === right.context.clientID,
    'clientID must be the same for all pushes',
  );
  assert(
    left.context.wsID === right.context.wsID,
    'wsID must be the same for all pushes',
  );
  assert(
    left.context.revision === right.context.revision,
    'revision must be the same for all pushes',
  );
  assert(
    authEquals(left.context.auth, right.context.auth),
    'auth must be the same for all pushes with the same clientID',
  );
  assert(
    left.push.schemaVersion === right.push.schemaVersion,
    'schemaVersion must be the same for all pushes with the same clientID',
  );
  assert(
    left.push.pushVersion === right.push.pushVersion,
    'pushVersion must be the same for all pushes with the same clientID',
  );
  assert(
    left.context.pushContext.headerOptions.cookie ===
      right.context.pushContext.headerOptions.cookie,
    'httpCookie must be the same for all pushes with the same clientID',
  );
  assert(
    left.context.pushContext.headerOptions.origin ===
      right.context.pushContext.headerOptions.origin,
    'origin must be the same for all pushes with the same clientID',
  );
  assert(
    left.context.userID === right.context.userID,
    'userID must be the same for all pushes with the same clientID',
  );
  assert(
    left.context.pushContext.url === right.context.pushContext.url,
    'userPushURL must be the same for all pushes with the same clientID',
  );
}
