import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import type {CommitResult} from './change-processor.ts';
import type {WriteWorkerClient} from './write-worker-client.ts';

// #6001: https://github.com/rocicorp/mono/pull/6001
// The v7 RM -> VS protocol reduces repeated stream overhead; this keeps the VS
// digestion side aligned by batching write-worker handoff around the same
// row-heavy transaction shape. Commit and rollback remain flush points so
// watermarks are only ACKed after the worker durably applies the ordered
// transaction segment.
export class WorkerMessageBatcher {
  readonly #worker: WriteWorkerClient;
  readonly #maxMessages: number;
  #messages: ChangeStreamData[] = [];

  constructor(worker: WriteWorkerClient, maxMessages: number) {
    this.#worker = worker;
    this.#maxMessages = maxMessages;
  }

  get size() {
    return this.#messages.length;
  }

  push(message: ChangeStreamData): Promise<CommitResult | null> | undefined {
    this.#messages.push(message);
    return this.#shouldFlush(message) ? this.flush() : undefined;
  }

  flush(): Promise<CommitResult | null> | undefined {
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
    return (
      message[0] === 'commit' ||
      message[0] === 'rollback' ||
      this.#messages.length >= this.#maxMessages
    );
  }
}
