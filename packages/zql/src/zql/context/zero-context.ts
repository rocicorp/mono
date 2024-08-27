import type {ExperimentalNoIndexDiff} from 'replicache';

import type {GotCallback, SubscriptionDelegate} from './context.js';
import {MemorySource} from '../ivm2/memory-source.js';
import {ValueType} from '../ivm2/schema.js';
import {Row} from '../ivm2/data.js';
import {Schema, toInputArgs} from '../query2/schema.js';
import {Host} from '../builder/builder.js';
import {Source} from '../ivm2/source.js';
import {AST} from '../ast2/ast.js';
import {Storage} from '../ivm2/operator.js';
import {MemoryStorage} from '../ivm2/memory-storage.js';

export type AddWatch = (name: string, cb: WatchCallback) => void;

export type WatchCallback = (changes: ExperimentalNoIndexDiff) => void;

export class ZeroContext implements Host {
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
  readonly #canonicalSource: MemorySource;

  constructor(
    name: string,
    columns: Record<string, ValueType>,
    primaryKeys: readonly string[],
    addWatch: AddWatch,
  ) {
    this.#canonicalSource = new MemorySource(name, columns, primaryKeys);
    addWatch(name, this.#handleDiff);
  }

  #handleDiff = (changes: ExperimentalNoIndexDiff) => {
    for (const diff of changes) {
      if (diff.op === 'del' || diff.op === 'change') {
        this.#canonicalSource.push({
          type: 'remove',
          row: diff.oldValue as Row,
        });
      }
      if (diff.op === 'add' || diff.op === 'change') {
        this.#canonicalSource.push({
          type: 'add',
          row: diff.newValue as Row,
        });
      }
    }
  };

  get() {
    return this.#canonicalSource;
  }
}
