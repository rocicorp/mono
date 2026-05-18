import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import type {CommitResult} from './change-processor.ts';
import type {WriteWorkerClient} from './write-worker-client.ts';

type ProcessMessagesResult = CommitResult | readonly CommitResult[] | null;

type WorkerMessageBatcherOptions = {
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
    return (
      (this.#flushOnCommit && message[0] === 'commit') ||
      message[0] === 'rollback' ||
      this.#messages.length >= this.#maxMessages
    );
  }
}
