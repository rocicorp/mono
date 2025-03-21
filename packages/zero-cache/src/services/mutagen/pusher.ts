import type {LogContext} from '@rocicorp/logger';
import {must} from '../../../../shared/src/must.ts';
import {Queue} from '../../../../shared/src/queue.ts';
import * as v from '../../../../shared/src/valita.ts';
import {
  pushResponseSchema,
  type PushBody,
  type PushResponse,
} from '../../../../zero-protocol/src/push.ts';
import type {Service} from '../service.ts';
import {type ZeroConfig} from '../../config/zero-config.ts';
import {upstreamSchema} from '../../types/shards.ts';
import type {HandlerResult} from '../../workers/connection.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {Subscription} from '../../types/subscription.ts';
import {groupBy} from '../../../../shared/src/arrays.ts';

export interface Pusher {
  enqueuePush(
    clientID: string,
    wsID: string,
    push: PushBody,
    jwt: string | undefined,
  ): HandlerResult;
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
  #stopped: Promise<void> | undefined;

  constructor(
    config: Config,
    lc: LogContext,
    clientGroupID: string,
    pushUrl: string,
    apiKey: string | undefined,
  ) {
    this.#queue = new Queue();
    this.#pusher = new PushWorker(config, lc, pushUrl, apiKey, this.#queue);
    this.id = clientGroupID;
  }

  enqueuePush(
    clientID: string,
    wsID: string,
    push: PushBody,
    jwt: string | undefined,
  ): HandlerResult {
    const downstream: Subscription<Downstream> | undefined =
      this.#pusher.maybeInitConnection(clientID, wsID);

    this.#queue.enqueue({push, jwt});

    if (downstream) {
      return {
        type: 'stream',
        source: 'pusher',
        stream: downstream,
      };
    }

    return {
      type: 'ok',
    };
  }

  run(): Promise<void> {
    this.#stopped = this.#pusher.run();
    return this.#stopped;
  }

  stop(): Promise<void> {
    this.#queue.enqueue('stop');
    return must(this.#stopped, 'Stop was called before `run`');
  }
}

type PusherEntry = {
  push: PushBody;
  jwt: string | undefined;
};
type PusherEntryOrStop = PusherEntry | 'stop';

/**
 * Awaits items in the queue then drains and sends them all
 * to the user's API server.
 */
class PushWorker {
  readonly #pushURL: string;
  readonly #apiKey: string | undefined;
  readonly #queue: Queue<PusherEntryOrStop>;
  readonly #lc: LogContext;
  readonly #config: Config;
  readonly #clients: Map<string, [wsID: string, Subscription<Downstream>]>;

  constructor(
    config: Config,
    lc: LogContext,
    pushURL: string,
    apiKey: string | undefined,
    queue: Queue<PusherEntryOrStop>,
  ) {
    this.#pushURL = pushURL;
    this.#apiKey = apiKey;
    this.#queue = queue;
    this.#lc = lc.withContext('component', 'pusher');
    this.#config = config;
    this.#clients = new Map();
  }

  maybeInitConnection(clientID: string, wsID: string) {
    const existing = this.#clients.get(clientID);
    if (existing && existing[0] === wsID) {
      // already initialized for this socket
      return undefined;
    }

    // client is back on a new connection
    if (existing) {
      existing[1].cancel();
    }

    const downstream = Subscription.create<Downstream>({
      cleanup: () => {
        this.#clients.delete(clientID);
      },
    });
    this.#clients.set(clientID, [wsID, downstream]);
    return downstream;
  }

  async run() {
    for (;;) {
      const task = await this.#queue.dequeue();
      const rest = this.#queue.drain();
      const [pushes, terminate] = combinePushes([task, ...rest]);
      for (const push of pushes) {
        const response = await this.#processPush(push);
        this.#fanOutResponses(response);
      }

      if (terminate) {
        break;
      }
    }
  }

  #fanOutResponses(response: PushResponse) {
    if ('error' in response) {
      const groupedMutationIDs = groupBy(
        response.mutationIDs ?? [],
        m => m.clientID,
      );
      for (const [clientID, mutationIDs] of groupedMutationIDs) {
        const client = this.#clients.get(clientID);
        if (client) {
          client[1].push([
            'push-response',
            {
              ...response,
              mutationIDs,
            },
          ]);
        }
      }
    } else {
      const groupedMutations = groupBy(response.mutations, m => m.id.clientID);
      for (const [clientID, mutations] of groupedMutations) {
        const client = this.#clients.get(clientID);
        if (client) {
          client[1].push(['push-response', {mutations}]);
        }
      }
    }
  }

  async #processPush(entry: PusherEntry): Promise<PushResponse> {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    };
    if (this.#apiKey) {
      headers['X-Api-Key'] = this.#apiKey;
    }
    if (entry.jwt) {
      headers['Authorization'] = `Bearer ${entry.jwt}`;
    }

    try {
      const params = new URLSearchParams();
      params.append(
        'schema',
        upstreamSchema({
          appID: this.#config.app.id,
          shardNum: this.#config.shard.num,
        }),
      );
      params.append('appID', this.#config.app.id);
      const response = await fetch(`${this.#pushURL}?${params.toString()}`, {
        method: 'POST',
        headers,
        body: JSON.stringify(entry.push),
      });
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
      return v.parse(json, pushResponseSchema);
    } catch (e) {
      // We do not kill the pusher on error.
      // If the user's API server is down, the mutations will never be acknowledged
      // and the client will eventually retry.
      this.#lc.error?.('failed to push', e);
      return {
        error: 'zero-pusher',
        details: String(e),
        mutationIDs: entry.push.mutations.map(m => ({
          id: m.id,
          clientID: m.clientID,
        })),
      };
    }
  }
}

/**
 * Scans over the array of pushes and puts consecutive pushes with the same JWT
 * into a single push.
 *
 * If a 'stop' is encountered, the function returns the accumulated pushes up
 * to that point and a boolean indicating that the pusher should stop.
 *
 * Exported for testing.
 *
 * Future optimization: every unique clientID will have the same JWT for all of its
 * pushes. Given that, we could combine pushes across clientIDs which would
 * create less fragmentation in the case where mutations among clients are interleaved.
 */
export function combinePushes(
  entries: readonly (PusherEntryOrStop | undefined)[],
): [PusherEntry[], boolean] {
  const ret: PusherEntry[] = [];

  for (const entry of entries) {
    if (entry === 'stop' || entry === undefined) {
      return [ret, true] as const;
    }

    if (ret.length === 0) {
      ret.push(entry);
      continue;
    }

    const last = ret[ret.length - 1];
    if (last.jwt === entry.jwt) {
      last.push.mutations.push(...entry.push.mutations);
    } else {
      ret.push(entry);
    }
  }

  return [ret, false] as const;
}
