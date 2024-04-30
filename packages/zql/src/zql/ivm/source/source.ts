import type {DifferenceStream} from '../graph/difference-stream.js';
import type {Version} from '../types.js';
import type {Request} from '../graph/message.js';

export interface Source<T extends object> {
  readonly stream: DifferenceStream<T>;
  add(value: T): this;
  delete(value: T): this;

  processMessage(message: Request): void;

  // We could remove `seed` and implicitly deduce it from the `add` method
  seed(values: Iterable<T>): this;
  isSeeded(): boolean;
  awaitSeeding(): PromiseLike<void>;
}

export interface SourceInternal {
  // Add values to queues
  onCommitEnqueue(version: Version): void;
  // Now that the graph has computed itself fully, notify effects / listeners
  onCommitted(version: Version): void;
  onRollback(): void;
}
