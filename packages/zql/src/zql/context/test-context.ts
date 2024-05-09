import {compareUTF8} from 'compare-utf8';
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
import type {Version} from '../ivm/types.js';
import type {Context} from './context.js';

export class TestContext implements Context {
  readonly materialite = new Materialite();
  readonly #sources = new Map<string, Source<object>>();

  subscriptionsChangedLog: {type: 'added' | 'removed'; ast: AST}[] = [];

  getSource<T extends Entity>(name: string): Source<T> {
    if (!this.#sources.has(name)) {
      const source = this.materialite.newSetSource(
        (l: T, r: T) => compareUTF8(l.id, r.id),
        [['id'], 'asc'],
      ) as unknown as Source<object>;
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

export class InfiniteSourceContext<T extends Entity> implements Context {
  readonly materialite = new Materialite();
  readonly #source: Source<T>;

  constructor(generator: {
    [Symbol.iterator](): Generator<Entry<T>, void, unknown>;
  }) {
    this.#source = this.materialite.constructSource(
      internal => new InfiniteSuorce(internal, generator),
    );
  }

  getSource<X extends Entity>(_name: string): Source<X> {
    return this.#source as unknown as Source<X>;
  }

  subscriptionAdded(_ast: AST): void {}

  subscriptionRemoved(_ast: AST): void {}
}

export function makeTestContext(): TestContext {
  return new TestContext();
}

export function makeInfiniteSourceContext<T extends Entity>(generator: {
  [Symbol.iterator](): Generator<Entry<T>, void, unknown>;
}): InfiniteSourceContext<T> {
  return new InfiniteSourceContext(generator);
}

class InfiniteSuorce<T extends object> implements Source<T> {
  readonly #materialite: MaterialiteForSourceInternal;
  readonly #stream: DifferenceStream<T>;
  readonly #internal: SourceInternal;
  readonly #generator: {
    [Symbol.iterator](): Generator<Entry<T>, void, unknown>;
  };

  constructor(
    materialite: MaterialiteForSourceInternal,
    generator: {
      [Symbol.iterator](): Generator<Entry<T>, void, unknown>;
    },
  ) {
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
      case 'pull': {
        this.#materialite.addDirtySource(this.#internal);
        this.#stream.newDifference(
          this.#materialite.getVersion(),
          this.#generator,
          createPullResponseMessage(message, [['id'], 'asc']),
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
