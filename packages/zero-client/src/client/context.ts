import type {NoIndexDiff} from '../../../replicache/src/btree/node.ts';
import {assert, unreachable} from '../../../shared/src/asserts.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {TableSchema} from '../../../zero-schema/src/table-schema.ts';
import {MemorySource} from '../../../zql/src/ivm/memory-source.ts';
import {MemoryStorage} from '../../../zql/src/ivm/memory-storage.ts';
import type {Storage} from '../../../zql/src/ivm/operator.ts';
import type {Source} from '../../../zql/src/ivm/source.ts';
import type {
  CommitListener,
  GotCallback,
  QueryDelegate,
} from '../../../zql/src/query/query-impl.ts';
import {ENTITIES_KEY_PREFIX} from './keys.ts';

export type AddQuery = (
  ast: AST,
  gotCallback?: GotCallback | undefined,
) => () => void;

/**
 * ZeroContext glues together zql and Replicache. It listens to changes in
 * Replicache data and pushes them into IVM and on tells the server about new
 * queries.
 */
export class ZeroContext implements QueryDelegate {
  // It is a bummer to have to maintain separate MemorySources here and copy the
  // data in from the Replicache db. But we want the data to be accessible via
  // pipelines *synchronously* and the core Replicache infra is all async. So
  // that needs to be fixed.
  readonly #sources = new Map<string, MemorySource | undefined>();
  readonly #tables: Record<string, TableSchema>;
  readonly #addQuery: AddQuery;
  readonly #batchViewUpdates: (applyViewUpdates: () => void) => void;
  readonly #commitListeners: Set<CommitListener> = new Set();

  readonly staticQueryParameters = undefined;

  constructor(
    tables: Record<string, TableSchema>,
    addQuery: AddQuery,
    batchViewUpdates: (applyViewUpdates: () => void) => void,
  ) {
    this.#tables = tables;
    this.#addQuery = addQuery;
    this.#batchViewUpdates = batchViewUpdates;
  }

  getSource(name: string): Source | undefined {
    if (this.#sources.has(name)) {
      return this.#sources.get(name);
    }

    const schema = this.#tables[name];
    const source = schema
      ? new MemorySource(name, schema.columns, schema.primaryKey)
      : undefined;
    this.#sources.set(name, source);
    return source;
  }

  addServerQuery(ast: AST, gotCallback?: GotCallback | undefined) {
    return this.#addQuery(ast, gotCallback);
  }

  createStorage(): Storage {
    return new MemoryStorage();
  }

  onTransactionCommit(cb: CommitListener): () => void {
    this.#commitListeners.add(cb);
    return () => {
      this.#commitListeners.delete(cb);
    };
  }

  batchViewUpdates<T>(applyViewUpdates: () => T) {
    let result: T | undefined;
    let viewChangesPerformed = false;
    this.#batchViewUpdates(() => {
      result = applyViewUpdates();
      viewChangesPerformed = true;
    });
    assert(
      viewChangesPerformed,
      'batchViewUpdates must call applyViewUpdates synchronously.',
    );
    return result as T;
  }

  processChanges(changes: NoIndexDiff) {
    try {
      this.batchViewUpdates(() => {
        for (const diff of changes) {
          const {key} = diff;
          assert(key.startsWith(ENTITIES_KEY_PREFIX));
          const slash = key.indexOf('/', ENTITIES_KEY_PREFIX.length);
          const name = key.slice(ENTITIES_KEY_PREFIX.length, slash);
          const source = this.getSource(name);
          if (!source) {
            continue;
          }

          switch (diff.op) {
            case 'del':
              assert(typeof diff.oldValue === 'object');
              source.push({
                type: 'remove',
                row: diff.oldValue as Row,
              });
              break;
            case 'add':
              assert(typeof diff.newValue === 'object');
              source.push({
                type: 'add',
                row: diff.newValue as Row,
              });
              break;
            case 'change':
              assert(typeof diff.newValue === 'object');
              assert(typeof diff.oldValue === 'object');

              // Edit changes are not yet supported everywhere. For now we only
              // generate them in tests.
              source.push({
                type: 'edit',
                row: diff.newValue as Row,
                oldRow: diff.oldValue as Row,
              });

              break;
            default:
              unreachable(diff);
          }
        }
      });
    } finally {
      this.#endTransaction();
    }
  }

  #endTransaction() {
    for (const listener of this.#commitListeners) {
      listener();
    }
  }
}
