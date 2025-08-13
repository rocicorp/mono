import type {LogContext} from '@rocicorp/logger';
import {groupBy} from '../../../../shared/src/arrays.ts';
import {assert} from '../../../../shared/src/asserts.ts';
import {must} from '../../../../shared/src/must.ts';
import {Queue} from '../../../../shared/src/queue.ts';
import * as v from '../../../../shared/src/valita.ts';
import type {UserMutateParams} from '../../../../zero-protocol/src/connect.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {ErrorKind} from '../../../../zero-protocol/src/error-kind.ts';
import {
  pushResponseSchema,
  type MutationID,
  type PushBody,
  type PushError,
  type PushResponse,
} from '../../../../zero-protocol/src/push.ts';
import {type ZeroConfig} from '../../config/zero-config.ts';
import {fetchFromAPIServer} from '../../custom/fetch.ts';
import {getOrCreateCounter} from '../../observability/metrics.ts';
import {recordMutation} from '../../server/anonymous-otel-start.ts';
import {ErrorForClient} from '../../types/error-for-client.ts';
import type {PostgresDB} from '../../types/pg.ts';
import {upstreamSchema} from '../../types/shards.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription, type Result} from '../../types/subscription.ts';
import type {HandlerResult, StreamResult} from '../../workers/connection.ts';
import type {RefCountedService, Service} from '../service.ts';

type Fatal = {
  error: 'forClient';
  cause: ErrorForClient;
  mutationIDs: PushError['mutationIDs'];
};

export interface Pusher extends RefCountedService {
  readonly pushURL: string | undefined;

  initConnection(
    clientID: string,
    wsID: string,
    userPushParams: UserMutateParams | undefined,
  ): Source<Downstream>;
  enqueuePush(
    clientID: string,
    push: PushBody,
    jwt: string | undefined,
    httpCookie: string | undefined,
  ): HandlerResult;
  ackMutationResponses(upToID: MutationID): Promise<void>;
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
  readonly #upstream: PostgresDB;
  readonly #config: Config;
  #stopped: Promise<void> | undefined;
  #refCount = 0;
  #isStopped = false;

  constructor(
    upstream: PostgresDB,
    appConfig: Config,
    pushConfig: ZeroConfig['push'] & {url: string[]},
    lc: LogContext,
    clientGroupID: string,
  ) {
    this.#config = appConfig;
    this.#upstream = upstream;
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
    userPushParams: UserMutateParams | undefined,
  ) {
    return this.#pusher.initConnection(clientID, wsID, userPushParams);
  }

  enqueuePush(
    clientID: string,
    push: PushBody,
    jwt: string | undefined,
    httpCookie: string | undefined,
  ): Exclude<HandlerResult, StreamResult> {
    if (!this.#pushConfig.forwardCookies) {
      httpCookie = undefined; // remove cookies if not forwarded
    }
    this.#queue.enqueue({push, jwt, clientID, httpCookie});

    return {
      type: 'ok',
    };
  }

  async ackMutationResponses(upToID: MutationID) {
    // delete the relevant rows from the `mutations` table
    const sql = this.#upstream;
    await sql`DELETE FROM ${sql(
      upstreamSchema({
        appID: this.#config.app.id,
        shardNum: this.#config.shard.num,
      }),
    )}.mutations WHERE "clientGroupID" = ${this.id} AND "clientID" = ${upToID.clientID} AND "mutationID" <= ${upToID.id}`;
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
  jwt: string | undefined;
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
  readonly #apiKey: string | undefined;
  readonly #queue: Queue<PusherEntryOrStop>;
  readonly #lc: LogContext;
  readonly #config: Config;
  readonly #clients: Map<
    string,
    {
      wsID: string;
      userParams?: UserMutateParams | undefined;
      downstream: Subscription<Downstream>;
    }
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
    pushURL: string[],
    apiKey: string | undefined,
    queue: Queue<PusherEntryOrStop>,
  ) {
    this.#pushURLs = pushURL;
    this.#apiKey = apiKey;
    this.#queue = queue;
    this.#lc = lc.withContext('component', 'pusher');
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
    userParams: UserMutateParams | undefined,
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

    const downstream = Subscription.create<Downstream>({
      cleanup: () => {
        this.#clients.delete(clientID);
      },
    });
    this.#clients.set(clientID, {wsID, downstream, userParams});
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
  #fanOutResponses(response: PushResponse | Fatal) {
    const responses: Promise<Result>[] = [];
    const connectionTerminations: (() => void)[] = [];

    // if the entire push failed, send that to the client.
    if ('error' in response) {
      this.#lc.warn?.(
        'The server behind ZERO_PUSH_URL returned a push error.',
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
        if (
          response.error === 'unsupportedPushVersion' ||
          response.error === 'unsupportedSchemaVersion'
        ) {
          client.downstream.fail(
            new ErrorForClient({
              kind: ErrorKind.InvalidPush,
              message: response.error,
            }),
          );
        } else if (response.error === 'forClient') {
          client.downstream.fail(response.cause);
        } else {
          responses.push(
            client.downstream.push([
              'pushResponse',
              {
                ...response,
                mutationIDs,
              },
            ]).result,
          );
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

        let failure: ErrorForClient | undefined;
        let i = 0;
        for (; i < mutations.length; i++) {
          const m = mutations[i];
          if ('error' in m.result) {
            this.#lc.warn?.(
              'The server behind ZERO_PUSH_URL returned a mutation error.',
              m.result,
            );
          }
          if ('error' in m.result && m.result.error === 'oooMutation') {
            failure = new ErrorForClient({
              kind: ErrorKind.InvalidPush,
              message: 'mutation was out of order',
            });
            break;
          }
        }

        if (failure && i < mutations.length - 1) {
          this.#lc.error?.(
            'push-response contains mutations after a mutation which should fatal the connection',
          );
        }

        if (failure) {
          connectionTerminations.push(() => client.downstream.fail(failure));
        }
      }
    }

    connectionTerminations.forEach(cb => cb());
  }

  async #processPush(entry: PusherEntry): Promise<PushResponse | Fatal> {
    this.#customMutations.add(entry.push.mutations.length, {
      clientGroupID: entry.push.clientGroupID,
    });
    this.#pushes.add(1, {
      clientGroupID: entry.push.clientGroupID,
    });

    // Record custom mutations for telemetry
    recordMutation('custom', entry.push.mutations.length);

    try {
      const response = await fetchFromAPIServer(
        must(this.#pushURLs[0], 'ZERO_MUTATE_URL is not set'),
        this.#pushURLs,
        {
          appID: this.#config.app.id,
          shardNum: this.#config.shard.num,
        },
        {
          apiKey: this.#apiKey,
          token: entry.jwt,
          cookie: entry.httpCookie,
        },
        this.#clients.get(entry.clientID)?.userParams?.queryParams,
        entry.push,
      );

      if (!response.ok) {
        return {
          error: 'http',
          status: response.status,
          details: await response.text(),
          mutationIDs: entry.push.mutations.map(m => ({
            id: m.id,
            clientID: m.clientID,
          })),
        };
      }

      const json = await response.json();
      try {
        return v.parse(json, pushResponseSchema);
      } catch (e) {
        this.#lc.error?.('failed to parse push response', JSON.stringify(json));
        throw e;
      }
    } catch (e) {
      const mutationIDs = entry.push.mutations.map(m => ({
        id: m.id,
        clientID: m.clientID,
      }));

      this.#lc.error?.('failed to push', e);
      if (e instanceof ErrorForClient) {
        return {
          error: 'forClient',
          cause: e,
          mutationIDs,
        };
      }

      // We do not kill the pusher on error.
      // If the user's API server is down, the mutations will never be acknowledged
      // and the client will eventually retry.
      return {
        error: 'zeroPusher',
        details: String(e),
        mutationIDs,
      };
    }
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
    left.jwt === right.jwt,
    'jwt must be the same for all pushes with the same clientID',
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
