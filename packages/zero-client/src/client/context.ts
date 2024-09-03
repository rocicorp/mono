import {ExperimentalNoIndexDiff} from 'replicache';
import {assert} from 'shared/src/asserts.js';
import {ChangeType} from 'zql/src/zql/ivm/change.js';
import {AST} from '../../../zql/src/zql/ast/ast.js';
import {Row} from '../../../zql/src/zql/ivm/data.js';
import {MemorySource} from '../../../zql/src/zql/ivm/memory-source.js';
import {MemoryStorage} from '../../../zql/src/zql/ivm/memory-storage.js';
import {Storage} from '../../../zql/src/zql/ivm/operator.js';
import {Source} from '../../../zql/src/zql/ivm/source.js';
import {
  CommitListener,
  QueryDelegate,
} from '../../../zql/src/zql/query/query-impl.js';
import {Schema} from '../../../zql/src/zql/query/schema.js';
import {ENTITIES_KEY_PREFIX} from './keys.js';

export type AddQuery = (ast: AST) => () => void;

/**
 * ZeroContext glues together zql and Replicache. It listens to changes in
 * Repliache data and pushes them into IVM and on tells the server about new
 * queries.
 */
export class ZeroContext implements QueryDelegate {
  // It is a bummer to have to maintain separate MemorySources here and copy the
  // data in from the Replicache db. But we want the data to be accessible via
  // pipelines *synchronously* and the core Replicache infra is all async. So
  // that needs to be fixed.
  readonly #sources = new Map<string, MemorySource>();
  readonly #schemas: Record<string, Schema>;
  readonly #addQuery: AddQuery;
  readonly #commitListeners: Set<CommitListener> = new Set();

  constructor(schemas: Record<string, Schema>, addQuery: AddQuery) {
    this.#schemas = schemas;
    this.#addQuery = addQuery;
  }

  getSource(name: string): Source {
    let source = this.#sources.get(name);
    if (source !== undefined) {
      return source;
    }
    const schema = this.#schemas[name] as Schema | undefined;
    if (!schema) {
      throw new Error(`No schema found for table ${name}`);
    }
    source = new MemorySource(name, schema.columns, schema.primaryKey);
    this.#sources.set(name, source);
    return source;
  }

  addServerQuery(ast: AST) {
    return this.#addQuery(ast);
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

  processChanges(changes: ExperimentalNoIndexDiff) {
    try {
      for (const diff of changes) {
        const {key} = diff;
        assert(key.startsWith(ENTITIES_KEY_PREFIX));
        const slash = key.indexOf('/', ENTITIES_KEY_PREFIX.length);
        const name = key.slice(ENTITIES_KEY_PREFIX.length, slash);
        const source = this.getSource(name);

        if (diff.op === 'del' || diff.op === 'change') {
          assert(typeof diff.oldValue === 'object');
          source.push({
            type: ChangeType.Remove,
            row: diff.oldValue as Row,
          });
        }
        if (diff.op === 'add' || diff.op === 'change') {
          assert(typeof diff.newValue === 'object');
          source.push({
            type: ChangeType.Add,
            row: diff.newValue as Row,
          });
        }
      }
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
