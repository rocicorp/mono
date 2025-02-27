import type {LogContext} from '@rocicorp/logger';
import {must} from '../../../../shared/src/must.ts';
import {Queue} from '../../../../shared/src/queue.ts';
import * as ErrorKind from '../../../../zero-protocol/src/error-kind-enum.ts';
import type {PushBody} from '../../../../zero-protocol/src/push.ts';
import type {Service} from '../service.ts';
import type {MutationError} from './mutagen.ts';

export interface Pusher {
  enqueuePush(push: PushBody, jwt: string | undefined): void;
}

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
export class PusherService implements Service {
  readonly id: string;
  readonly #pusher: PushWorker;
  readonly #queue: Queue<PusherEntryOrStop>;
  #stopped: Promise<void> | undefined;

  constructor(
    lc: LogContext,
    clientGroupID: string,
    pushUrl: string,
    apiKey: string | undefined,
  ) {
    this.#queue = new Queue();
    this.#pusher = new PushWorker(lc, pushUrl, apiKey, this.#queue);
    this.id = clientGroupID;
  }

  enqueuePush(push: PushBody, jwt: string | undefined) {
    this.#queue.enqueue({push, jwt});
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

  constructor(
    lc: LogContext,
    pushURL: string,
    apiKey: string | undefined,
    queue: Queue<PusherEntryOrStop>,
  ) {
    this.#pushURL = pushURL;
    this.#apiKey = apiKey;
    this.#queue = queue;
    this.#lc = lc.withContext('component', 'pusher');
  }

  async run() {
    this.#lc.info?.('starting pusher');
    for (;;) {
      const task = await this.#queue.dequeue();
      this.#lc.info?.('dequeued a push task');
      const rest = this.#queue.drain();
      const [pushes, terminate] = combinePushes([task, ...rest]);
      for (const push of pushes) {
        await this.#processPush(push);
      }

      if (terminate) {
        break;
      }
    }
  }

  async #processPush(entry: PusherEntry): Promise<MutationError | undefined> {
    this.#lc.info?.('processing custom mutation');
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
      this.#lc.info?.('FETCHING', this.#pushURL, JSON.stringify(entry.push));
      const response = await fetch(this.#pushURL, {
        method: 'POST',
        headers,
        body: JSON.stringify(entry.push),
      });
      // TODO: handle more varied response types from the user's API server
      if (!response.ok) {
        this.#lc.error?.('failed to push', response);
        return [
          ErrorKind.MutationFailed,
          `API server failed to process mutation: ${response.status} ${response.statusText}`,
        ];
      }
    } catch (e) {
      // We do not kill the pusher on error.
      // If the user's API server is down, the mutations will never be acknowledged
      // and the client will eventually retry.
      this.#lc.error?.('failed to push', e);
      return [
        ErrorKind.MutationFailed,
        e instanceof Error ? e.message : 'unknown error',
      ];
    }

    this.#lc.info?.('processed a custom mutation');
    return undefined;
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
