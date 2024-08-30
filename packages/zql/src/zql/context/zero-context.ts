import type {ExperimentalNoIndexDiff} from 'replicache';

import type {GotCallback, SubscriptionDelegate} from './context.js';
import {MemorySource} from '../ivm/memory-source.js';
import {ValueType} from '../ivm/schema.js';
import {Row} from '../ivm/data.js';
import {Schema, toInputArgs} from '../query/schema.js';
import {BuilderDelegate} from '../builder/builder.js';
import {Source} from '../ivm/source.js';
import {AST} from '../ast/ast.js';
import {Storage} from '../ivm/operator.js';
import {MemoryStorage} from '../ivm/memory-storage.js';
import {assert} from 'shared/src/asserts.js';

export type AddWatch = (name: string, cb: WatchCallback) => void;

export type WatchCallback = (changes: ExperimentalNoIndexDiff) => void;

export class ZeroContext implements BuilderDelegate {
  readonly #sourceStore: ZeroSourceStore;
  readonly #subscriptionDelegate: SubscriptionDelegate;
  readonly #schemas: Record<string, Schema>;

  constructor(
    schemas: Record<string, Schema>,
    addWatch: AddWatch,
    subscriptionDelegate: SubscriptionDelegate,
  ) {
    this.#schemas = schemas;
    this.#sourceStore = new ZeroSourceStore(addWatch);
    this.#subscriptionDelegate = subscriptionDelegate;
  }

  getSource(name: string): Source {
    const schema = this.#schemas[name];
    const sourceArgs = toInputArgs(schema);
    return this.#sourceStore.getSource(
      name,
      sourceArgs.columns,
      sourceArgs.primaryKey,
    );
  }

  subscriptionAdded(ast: AST, gotCallback: GotCallback): () => void {
    return this.#subscriptionDelegate.subscriptionAdded(ast, gotCallback);
  }

  createStorage(): Storage {
    return new MemoryStorage();
  }
}

/**
 * Forwards Replicache changes to ZQL sources so they can be fed into any
 * queries that may exist.
 */
class ZeroSourceStore {
  readonly #sources = new Map<string, ZeroSource>();
  readonly #addWatch: AddWatch;

  constructor(addWatch: AddWatch) {
    this.#addWatch = addWatch;
  }

  getSource(
    name: string,
    columns: Record<string, ValueType>,
    primaryKeys: readonly string[],
  ) {
    let source = this.#sources.get(name);
    if (source === undefined) {
      source = new ZeroSource(name, columns, primaryKeys, this.#addWatch);
      this.#sources.set(name, source);
    }

    return source.get();
  }
}

class ZeroSource {
  readonly #source: MemorySource;

  constructor(
    name: string,
    columns: Record<string, ValueType>,
    primaryKeys: readonly string[],
    addWatch: AddWatch,
  ) {
    this.#source = new MemorySource(name, columns, primaryKeys);
    addWatch(name, this.#handleDiff);
  }

  #handleDiff = (changes: ExperimentalNoIndexDiff) => {
    for (const diff of changes) {
      if (diff.op === 'del' || diff.op === 'change') {
        assert(typeof diff.oldValue === 'object');
        this.#source.push({
          type: 'remove',
          row: diff.oldValue as Row,
        });
      }
      if (diff.op === 'add' || diff.op === 'change') {
        assert(typeof diff.newValue === 'object');
        this.#source.push({
          type: 'add',
          row: diff.newValue as Row,
        });
      }
    }
  };

  get() {
    return this.#source;
  }
}
