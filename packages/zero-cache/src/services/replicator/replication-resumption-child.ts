import {pid} from 'node:process';
import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import type {LogConfig} from '../../../../shared/src/logging.ts';
import {Queue} from '../../../../shared/src/queue.ts';
import {parentWorker, type Worker} from '../../types/processes.ts';
import type {Source} from '../../types/streams.ts';
import {getPragmaConfig, setupReplica} from '../../workers/replicator.ts';
import type {
  ChangeStreamer,
  SerializedDownstream,
  SubscriberContext,
} from '../change-streamer/change-streamer.ts';
import {exitAfter, runUntilKilled} from '../life-cycle.ts';
import type {CommitResult} from './change-processor.ts';
import {ReplicatorService} from './replicator.ts';
import {
  ThreadWriteWorkerClient,
  type PragmaConfig,
  type SerializedChangeStreamData,
  type WriteWorkerClient,
} from './write-worker-client.ts';

type ChildFailpoint = 'none' | 'after-sqlite-commit-before-ack';

type ParentMessage =
  | [
      'replication-resumption:downstream',
      {seq: number; msg: SerializedDownstream},
    ]
  | ['replication-resumption:source-error', {message: string}]
  | ['replication-resumption:source-end', Record<string, never>];

type ChildMessage =
  | ['replication-resumption:ready', {pid: number}]
  | ['replication-resumption:subscribe', SubscriberContext]
  | ['replication-resumption:consumed', {seq: number}]
  | ['replication-resumption:cancel', Record<string, never>]
  | [
      'replication-resumption:failpoint',
      {name: Exclude<ChildFailpoint, 'none'>; watermark: string},
    ];

const lc = new LogContext('error');

function send(parent: Worker, msg: ChildMessage): void {
  parent.send(msg as never);
}

function parentMessage(data: unknown): ParentMessage | undefined {
  if (!Array.isArray(data) || data.length !== 2) {
    return undefined;
  }
  switch (data[0]) {
    case 'replication-resumption:downstream':
    case 'replication-resumption:source-error':
    case 'replication-resumption:source-end':
      return data as ParentMessage;
  }
  return undefined;
}

class IPCChangeStreamer implements ChangeStreamer {
  readonly #parent: Worker;

  constructor(parent: Worker) {
    this.#parent = parent;
  }

  subscribe(ctx: SubscriberContext): Promise<Source<SerializedDownstream>> {
    send(this.#parent, ['replication-resumption:subscribe', ctx]);
    return Promise.resolve(new IPCDownstreamSource(this.#parent));
  }
}

type QueueEntry =
  | {type: 'message'; seq: number; msg: SerializedDownstream}
  | {type: 'end'};

class IPCDownstreamSource implements Source<SerializedDownstream> {
  readonly #parent: Worker;
  readonly #queue = new Queue<QueueEntry>();
  #canceled = false;

  constructor(parent: Worker) {
    this.#parent = parent;
    this.#parent.on('message', this.#onMessage);
  }

  readonly #onMessage = (data: unknown) => {
    const msg = parentMessage(data);
    if (!msg) {
      return;
    }
    switch (msg[0]) {
      case 'replication-resumption:downstream':
        this.#queue.enqueue({type: 'message', ...msg[1]});
        break;
      case 'replication-resumption:source-error':
        this.#queue.enqueueRejection(new Error(msg[1].message));
        break;
      case 'replication-resumption:source-end':
        this.#queue.enqueue({type: 'end'});
        break;
    }
  };

  cancel(): void {
    if (this.#canceled) {
      return;
    }
    this.#canceled = true;
    this.#parent.off('message', this.#onMessage);
    send(this.#parent, ['replication-resumption:cancel', {}]);
    this.#queue.enqueue({type: 'end'});
  }

  [Symbol.asyncIterator](): AsyncIterator<SerializedDownstream> {
    let previousSeq: number | undefined;
    return {
      next: async () => {
        if (previousSeq !== undefined) {
          send(this.#parent, [
            'replication-resumption:consumed',
            {seq: previousSeq},
          ]);
          previousSeq = undefined;
        }

        const entry = await this.#queue.dequeue();
        if (entry.type === 'end') {
          return {value: undefined, done: true};
        }
        previousSeq = entry.seq;
        return {value: entry.msg, done: false};
      },
      return: value => {
        if (previousSeq !== undefined) {
          send(this.#parent, [
            'replication-resumption:consumed',
            {seq: previousSeq},
          ]);
          previousSeq = undefined;
        }
        this.cancel();
        return Promise.resolve({value, done: true});
      },
    };
  }
}

class FailpointWriteWorkerClient implements WriteWorkerClient {
  readonly #inner: ThreadWriteWorkerClient;
  readonly #parent: Worker;
  readonly #failpoint: ChildFailpoint;
  #triggered = false;

  constructor(
    inner: ThreadWriteWorkerClient,
    parent: Worker,
    failpoint: ChildFailpoint,
  ) {
    this.#inner = inner;
    this.#parent = parent;
    this.#failpoint = failpoint;
  }

  init(
    dbPath: string,
    mode: Parameters<ThreadWriteWorkerClient['init']>[1],
    pragmas: PragmaConfig,
    logConfig: LogConfig,
  ) {
    return this.#inner.init(dbPath, mode, pragmas, logConfig);
  }

  getSubscriptionState() {
    return this.#inner.getSubscriptionState();
  }

  async processMessage(
    downstream: SerializedChangeStreamData,
  ): Promise<CommitResult | null> {
    const result = await this.#inner.processMessage(downstream);
    if (
      result?.watermark &&
      this.#failpoint === 'after-sqlite-commit-before-ack' &&
      !this.#triggered
    ) {
      this.#triggered = true;
      send(this.#parent, [
        'replication-resumption:failpoint',
        {name: this.#failpoint, watermark: result.watermark},
      ]);
      await resolver<never>().promise;
    }
    return result;
  }

  abort() {
    this.#inner.abort();
  }

  stop() {
    return this.#inner.stop();
  }

  onError(handler: (err: Error) => void) {
    this.#inner.onError(handler);
  }
}

function parseFailpoint(value: string | undefined): ChildFailpoint {
  switch (value) {
    case undefined:
    case 'none':
      return 'none';
    case 'after-sqlite-commit-before-ack':
      return value;
  }
  throw new Error(`unknown replication resumption failpoint: ${value}`);
}

export default async function runWorker(
  parent: Worker,
  _env: NodeJS.ProcessEnv,
  dbPath: string,
  failpointArg?: string,
): Promise<void> {
  const failpoint = parseFailpoint(failpointArg);
  await setupReplica(lc, 'serving', {file: dbPath});

  const worker = new FailpointWriteWorkerClient(
    new ThreadWriteWorkerClient(),
    parent,
    failpoint,
  );
  await worker.init(dbPath, 'serving', getPragmaConfig('serving'), {
    level: 'error',
    format: 'text',
  });

  const replicator = new ReplicatorService(
    lc,
    'replication-resumption-test',
    `replication-resumption-child-${pid}`,
    'serving',
    new IPCChangeStreamer(parent),
    worker,
    null,
  );

  const running = runUntilKilled(lc, parent, replicator);

  for await (const _ of replicator.subscribe()) {
    send(parent, ['replication-resumption:ready', {pid}]);
    break;
  }

  return running;
}

if (parentWorker) {
  const parent = parentWorker;
  const [dbPath, failpointArg] = process.argv.slice(2);
  if (!dbPath) {
    throw new Error('replication resumption child requires a replica db path');
  }
  void exitAfter(lc, () =>
    runWorker(parent, process.env, dbPath, failpointArg),
  );
}
