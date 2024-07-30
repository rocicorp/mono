import type {DifferenceStream} from '../graph/difference-stream.js';
import type {PipelineEntity, Version} from '../types.js';
import type {Request} from '../graph/message.js';
import type {HashIndex} from './source-hash-index.js';
import type {Primitive, Selector} from '../../ast/ast.js';

export interface Source<T extends PipelineEntity> {
  readonly stream: DifferenceStream<T>;
  add(value: T): this;
  delete(value: T): this;

  processMessage(message: Request): void;
  getOrCreateAndMaintainNewHashIndex<K extends Primitive>(
    column: Selector,
  ): HashIndex<K, T>;

  // We could remove `seed` and implicitly deduce it from the `add` method
  seed(values: Iterable<T>, derived: boolean): this;
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
