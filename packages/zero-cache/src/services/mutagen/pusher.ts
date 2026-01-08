import type {LogContext} from '@rocicorp/logger';
import {groupBy} from '../../../../shared/src/arrays.ts';
import {assert, unreachable} from '../../../../shared/src/asserts.ts';
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
import {
  CLEANUP_RESULTS_MUTATION_NAME,
  pushResponseSchema,
  type MutationID,
  type PushBody,
  type PushResponse,
} from '../../../../zero-protocol/src/push.ts';
import * as MutationType from '../../../../zero-protocol/src/mutation-type-enum.ts';
import {type ZeroConfig} from '../../config/zero-config.ts';
import {compileUrlPattern, fetchFromAPIServer} from '../../custom/fetch.ts';
import {getOrCreateCounter} from '../../observability/metrics.ts';
import {recordMutation} from '../../server/anonymous-otel-start.ts';
import {ProtocolErrorWithLevel} from '../../types/error-with-level.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import type {HandlerResult, StreamResult} from '../../workers/connection.ts';
import type {RefCountedService, Service} from '../service.ts';

export interface Pusher extends RefCountedService {
  readonly pushURL: string | undefined;

  initConnection(
    clientID: string,
    wsID: string,
    userPushURL: string | undefined,
  ): Source<Downstream>;
  enqueuePush(
    clientID: string,
    push: PushBody,
    auth: string | undefined,
    httpCookie: string | undefined,
  ): HandlerResult;
  ackMutationResponses(upToID: MutationID): Promise<void>;
  deleteClientMutations(clientIDs: string[]): Promise<void>;
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
  readonly #pusher: PushWorker;
  readonly #queue: Queue<PusherEntryOrStop>;
  readonly #pushConfig: ZeroConfig['push'] & {url: string[]};
  readonly #config: Config;
  readonly #lc: LogContext;
  readonly #pushURLPatterns: URLPattern[];
  #stopped: Promise<void> | undefined;
  #refCount = 0;
  #isStopped = false;

  constructor(
    appConfig: Config,
    pushConfig: ZeroConfig['push'] & {url: string[]},
    lc: LogContext,
    clientGroupID: string,
  ) {
    this.#config = appConfig;
    this.#lc = lc.withContext('component', 'pusherService');
    this.#pushURLPatterns = pushConfig.url.map(compileUrlPattern);
    this.#queue = new Queue();
    this.#pusher = new PushWorker(
      appConfig,
      lc,
      pushConfig.url,
      pushConfig.apiKey,
      this.#queue,
    );
    this.id = clientGroupID;
    this.#pushConfig = pushConfig;
  }

  get pushURL(): string | undefined {
    return this.#pusher.pushURL[0];
  }

  initConnection(
    clientID: string,
    wsID: string,
    userPushURL: string | undefined,
  ) {
    return this.#pusher.initConnection(clientID, wsID, userPushURL);
  }

  enqueuePush(
    clientID: string,
    push: PushBody,
    auth: string | undefined,
    httpCookie: string | undefined,
  ): Exclude<HandlerResult, StreamResult> {
    if (!this.#pushConfig.forwardCookies) {
      httpCookie = undefined; // remove cookies if not forwarded
    }
    this.#queue.enqueue({push, auth, clientID, httpCookie});

    return {
      type: 'ok',
    };
  }

  async ackMutationResponses(upToID: MutationID) {
    const url = this.#pushConfig.url[0];
    if (!url) {
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
        pushResponseSchema,
        'push',
        this.#lc,
        url,
        false,
        this.#pushURLPatterns,
        {appID: this.#config.app.id, shardNum: this.#config.shard.num},
        {apiKey: this.#pushConfig.apiKey},
        cleanupBody,
      );
    } catch (e) {
      this.#lc.warn?.('Failed to send cleanup mutation', {
        error: getErrorMessage(e),
      });
    }
  }

  async deleteClientMutations(clientIDs: string[]) {
    if (clientIDs.length === 0) {
      return;
    }
    const url = this.#pushConfig.url[0];
    if (!url) {
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
        pushResponseSchema,
        'push',
        this.#lc,
        url,
        false,
        this.#pushURLPatterns,
        {appID: this.#config.app.id, shardNum: this.#config.shard.num},
        {apiKey: this.#pushConfig.apiKey},
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
  auth: string | undefined;
  httpCookie: string | undefined;
  clientID: string;
};
type PusherEntryOrStop = PusherEntry | 'stop';

/**
 * Awaits items in the queue then drains and sends them all
 * to the user's API server.
 */
class PushWorker {
  readonly #pushURLs: string[];
  readonly #pushURLPatterns: URLPattern[];
  readonly #apiKey: string | undefined;
  readonly #queue: Queue<PusherEntryOrStop>;
  readonly #lc: LogContext;
  readonly #config: Config;
  readonly #clients: Map<
    string,
    {
      wsID: string;
      downstream: Subscription<Downstream>;
    }
  >;
  #userPushURL?: string | undefined;

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
    pushURL: string[],
    apiKey: string | undefined,
    queue: Queue<PusherEntryOrStop>,
  ) {
    this.#pushURLs = pushURL;
    this.#lc = lc.withContext('component', 'pusher');
    this.#pushURLPatterns = pushURL.map(compileUrlPattern);
    this.#apiKey = apiKey;
    this.#queue = queue;
    this.#config = config;
    this.#clients = new Map();
  }

  get pushURL() {
    return this.#pushURLs;
  }

  /**
   * Returns a new downstream stream if the clientID,wsID pair has not been seen before.
   * If a clientID already exists with a different wsID, that client's downstream is cancelled.
   */
  initConnection(
    clientID: string,
    wsID: string,
    userPushURL: string | undefined,
  ) {
    const existing = this.#clients.get(clientID);
    if (existing && existing.wsID === wsID) {
      // already initialized for this socket
      throw new Error('Connection was already initialized');
    }

    // client is back on a new connection
    if (existing) {
      existing.downstream.cancel();
    }

    // Handle client group level URL parameters
    if (this.#userPushURL === undefined) {
      // First client in the group - store its URL
      this.#userPushURL = userPushURL;
    } else {
      // Validate that subsequent clients have compatible parameters
      if (this.#userPushURL !== userPushURL) {
        this.#lc.warn?.(
          'Client provided different mutate parameters than client group',
          {
            clientID,
            clientURL: userPushURL,
            clientGroupURL: this.#userPushURL,
          },
        );
      }
    }

    const downstream = Subscription.create<Downstream>({
      cleanup: () => {
        this.#clients.delete(clientID);
      },
    });
    this.#clients.set(clientID, {wsID, downstream});
    return downstream;
  }

  async run() {
    for (;;) {
      const task = await this.#queue.dequeue();
      const rest = this.#queue.drain();
      const [pushes, terminate] = combinePushes([task, ...rest]);
      for (const push of pushes) {
        const response = await this.#processPush(push);
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
  #fanOutResponses(response: PushResponse) {
    const connectionTerminations: (() => void)[] = [];

    // if the entire push failed, send that to the client.
    if ('kind' in response || 'error' in response) {
      this.#lc.warn?.(
        'The server behind ZERO_MUTATE_URL returned a push error.',
        response,
      );
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
        } else if ('kind' in response) {
          this.#failDownstream(client.downstream, response);
        } else {
          unreachable(response);
        }
      }
    } else {
      // Look for mutations results that should cause us to terminate the connection
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

  async #processPush(entry: PusherEntry): Promise<PushResponse> {
    this.#customMutations.add(entry.push.mutations.length, {
      clientGroupID: entry.push.clientGroupID,
    });
    this.#pushes.add(1, {
      clientGroupID: entry.push.clientGroupID,
    });

    // Record custom mutations for telemetry
    recordMutation('custom', entry.push.mutations.length);

    const url =
      this.#userPushURL ??
      must(this.#pushURLs[0], 'ZERO_MUTATE_URL is not set');

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

      return await fetchFromAPIServer(
        pushResponseSchema,
        'push',
        this.#lc,
        url,
        url === this.#userPushURL,
        this.#pushURLPatterns,
        {
          appID: this.#config.app.id,
          shardNum: this.#config.shard.num,
        },
        {
          apiKey: this.#apiKey,
          token: entry.auth,
          cookie: entry.httpCookie,
        },
        entry.push,
      );
    } catch (e) {
      if (isProtocolError(e) && e.errorBody.kind === ErrorKind.PushFailed) {
        return {
          ...e.errorBody,
          mutationIDs,
        } as const satisfies PushFailedBody;
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
    const logLevel = errorBody.origin === ErrorOrigin.Server ? 'warn' : 'error';
    downstream.fail(new ProtocolErrorWithLevel(errorBody, logLevel));
  }
}

/**
 * Pushes for different clientIDs could theoretically be interleaved.
 *
 * In order to do efficient batching to the user's API server,
 * we collect all pushes for the same clientID into a single push.
 */
export function combinePushes(
  entries: readonly (PusherEntryOrStop | undefined)[],
): [PusherEntry[], boolean] {
  const pushesByClientID = new Map<string, PusherEntry[]>();

  function collect() {
    const ret: PusherEntry[] = [];
    for (const entries of pushesByClientID.values()) {
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

    const {clientID} = entry;
    const existing = pushesByClientID.get(clientID);
    if (existing) {
      existing.push(entry);
    } else {
      pushesByClientID.set(clientID, [entry]);
    }
  }

  return [collect(), false] as const;
}

// These invariants should always be true for a given clientID.
// If they are not, we have a bug in the code somewhere.
function assertAreCompatiblePushes(left: PusherEntry, right: PusherEntry) {
  assert(
    left.clientID === right.clientID,
    'clientID must be the same for all pushes',
  );
  assert(
    left.auth === right.auth,
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
}
