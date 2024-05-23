import {compareUTF8} from 'compare-utf8';
import {must} from 'shared/src/must.js';
import type {Entity} from '../../entity.js';
import type {AST} from '../ast/ast.js';
import {DifferenceStream} from '../ivm/graph/difference-stream.js';
import {createPullResponseMessage, Request} from '../ivm/graph/message.js';
import {
  Materialite,
  type MaterialiteForSourceInternal,
} from '../ivm/materialite.js';
import type {Entry} from '../ivm/multiset.js';
import type {Source, SourceInternal} from '../ivm/source/source.js';
import type {PipelineEntity, Version} from '../ivm/types.js';
import type {Context} from './context.js';

export class TestContext implements Context {
  readonly materialite = new Materialite();
  readonly #sources = new Map<string, Source<PipelineEntity>>();

  subscriptionsChangedLog: {type: 'added' | 'removed'; ast: AST}[] = [];

  getSource<T extends Entity>(name: string): Source<T> {
    if (!this.#sources.has(name)) {
      const source = this.materialite.newSetSource(
        (l: T, r: T) => compareUTF8(l.id, r.id),
        [[[name, 'id']], 'asc'],
        name,
      ) as unknown as Source<PipelineEntity>;
      source.seed([]);
      this.#sources.set(name, source);
    }
    return this.#sources.get(name)! as unknown as Source<T>;
  }

  subscriptionAdded(ast: AST): void {
    this.subscriptionsChangedLog.push({type: 'added', ast});
  }

  subscriptionRemoved(ast: AST): void {
    this.subscriptionsChangedLog.push({type: 'removed', ast});
  }
}

export class InfiniteSourceContext implements Context {
  readonly materialite = new Materialite();
  readonly #sources = new Map<string, Source<PipelineEntity>>();
  readonly #iterables = new Map<string, Iterable<Entry<PipelineEntity>>>();

  constructor(iterables: Map<string, Iterable<Entry<PipelineEntity>>>) {
    this.#iterables = iterables;
  }

  getSource<X extends Entity>(name: string): Source<X> {
    const existing = this.#sources.get(name);
    if (existing) {
      return existing as unknown as Source<X>;
    }

    let source: Source<PipelineEntity>;
    if (this.#iterables.has(name)) {
      source = this.materialite.constructSource(
        internal =>
          new InfiniteSource(internal, must(this.#iterables.get(name)), name),
      );
    } else {
      source = this.materialite.newSetSource<X>(
        (l: X, r: X) => compareUTF8(l.id, r.id),
        [[[name, 'id']], 'asc'],
        name,
      ) as unknown as Source<PipelineEntity>;
      source.seed([]);
      this.#sources.set(name, source);
    }
    this.#sources.set(name, source);

    return source as unknown as Source<X>;
  }

  subscriptionAdded(_ast: AST): void {}

  subscriptionRemoved(_ast: AST): void {}
}

export function makeTestContext(): TestContext {
  return new TestContext();
}

export function makeInfiniteSourceContext(
  iterables: Map<string, Iterable<Entry<PipelineEntity>>>,
): InfiniteSourceContext {
  return new InfiniteSourceContext(iterables);
}

class InfiniteSource<T extends PipelineEntity> implements Source<T> {
  readonly #materialite: MaterialiteForSourceInternal;
  readonly #stream: DifferenceStream<T>;
  readonly #internal: SourceInternal;
  readonly #generator: {
    [Symbol.iterator](): Iterator<Entry<T>>;
  };
  readonly #name;

  constructor(
    materialite: MaterialiteForSourceInternal,
    generator: Iterable<Entry<T>>,
    name: string,
  ) {
    this.#name = name;
    this.#materialite = materialite;
    this.#generator = generator;
    this.#stream = new DifferenceStream<T>();
    this.#stream.setUpstream({
      commit: () => {},
      messageUpstream: (message: Request) => {
        this.processMessage(message);
      },
      destroy: () => {},
    });

    this.#internal = {
      onCommitEnqueue: (_: Version) => {},
      onCommitted: (version: Version) => {
        this.#stream.commit(version);
      },
      onRollback: () => {},
    };
  }

  get stream(): DifferenceStream<T> {
    return this.#stream;
  }

  seed(_values: Iterable<T>): this {
    return this;
  }

  add(v: T): this {
    this.#stream.newDifference(
      this.#materialite.getVersion(),
      [[v, 1]],
      undefined,
    );
    this.#materialite.addDirtySource(this.#internal);
    return this;
  }

  delete(v: T): this {
    this.#stream.newDifference(
      this.#materialite.getVersion(),
      [[v, -1]],
      undefined,
    );
    this.#materialite.addDirtySource(this.#internal);
    return this;
  }

  processMessage(message: Request): void {
    switch (message.type) {
      // TODO: check for alternative order in the message.
      // create the source in the new order.
      // send from that source.
      case 'pull': {
        this.#materialite.addDirtySource(this.#internal);
        this.#stream.newDifference(
          this.#materialite.getVersion(),
          this.#generator,
          createPullResponseMessage(message, this.#name, [
            [[this.#name, 'id']],
            'asc',
          ]),
        );
        break;
      }
    }
  }

  isSeeded(): boolean {
    return true;
  }

  awaitSeeding(): PromiseLike<void> {
    return Promise.resolve();
  }

  on(_cb: (value: T, version: Version) => void): void {}

  destroy(): void {}
}
