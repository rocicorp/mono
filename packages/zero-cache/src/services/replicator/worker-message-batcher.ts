import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import type {CommitResult} from './change-processor.ts';
import type {WriteWorkerClient} from './write-worker-client.ts';

type ProcessMessagesResult = CommitResult | readonly CommitResult[] | null;

type WorkerMessageBatcherOptions = {
  // Legacy subscribe() delivers one message at a time, so commits still need to
  // flush immediately to keep replication status and backfill completion moving.
  // Batched subscribe() preserves RM websocket frames and flushes after the frame.
  readonly flushOnCommit?: boolean | undefined;
};

// #6001: https://github.com/rocicorp/mono/pull/6001
// RM -> VS traffic arrives in ordered stream batches. Grouping write-worker
// handoff around those batches keeps row-heavy transactions from paying a
// promise/IPC boundary per row; callers that need per-commit ACKs can leave
// flushOnCommit enabled.
export class WorkerMessageBatcher {
  readonly #worker: WriteWorkerClient;
  readonly #maxMessages: number;
  readonly #flushOnCommit: boolean;
  #messages: ChangeStreamData[] = [];

  constructor(
    worker: WriteWorkerClient,
    maxMessages: number,
    options: WorkerMessageBatcherOptions = {},
  ) {
    this.#worker = worker;
    this.#maxMessages = maxMessages;
    this.#flushOnCommit = options.flushOnCommit ?? true;
  }

  get size() {
    return this.#messages.length;
  }

  push(message: ChangeStreamData): Promise<ProcessMessagesResult> | undefined {
    this.#messages.push(message);
    return this.#shouldFlush(message) ? this.flush() : undefined;
  }

  flush(): Promise<ProcessMessagesResult> | undefined {
    if (this.#messages.length === 0) {
      return undefined;
    }
    const messages = this.#messages;
    this.#messages = [];
    return this.#worker.processMessages(messages);
  }

  clear() {
    this.#messages = [];
  }

  #shouldFlush(message: ChangeStreamData) {
    const tag = message[0];
    if (this.#flushOnCommit && tag === 'commit') {
      // A commit is the first point where the write worker can return a durable
      // watermark, schema-update, or backfill-complete result. Callers on the
      // one-message-at-a-time path must see that result before later stream work.
      return true;
    }
    if (tag === 'rollback') {
      // Rollback terminates the current upstream transaction without a commit
      // result. Flushing here keeps rolled-back rows from sharing a worker call
      // with the next transaction, which preserves the stream's transaction shape.
      return true;
    }
    if (this.#messages.length >= this.#maxMessages) {
      // The batcher is meant to remove per-row worker calls, not to become an
      // unbounded in-memory queue if a very large transaction arrives.
      return true;
    }
    return false;
  }
}
