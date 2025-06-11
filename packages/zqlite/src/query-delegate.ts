import type {LogContext} from '@rocicorp/logger';
import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import type {Database} from './db.ts';
import type {Source} from '../../zql/src/ivm/source.ts';
import {TableSource} from './table-source.ts';
import type {LogConfig} from '../../otel/src/log-options.ts';
import type {Input} from '../../zql/src/ivm/operator.ts';
import {MemoryStorage} from '../../zql/src/ivm/memory-storage.ts';
import type {FilterInput} from '../../zql/src/ivm/filter-operators.ts';
import type {
  CommitListener,
  QueryDelegate,
} from '../../zql/src/query/query-delegate.ts';

export class QueryDelegateImpl implements QueryDelegate {
  readonly #lc: LogContext;
  readonly #db: Database;
  readonly #schema: Schema;
  readonly #sources: Map<string, Source>;
  readonly #logConfig: LogConfig;
  readonly defaultQueryComplete = true;
  readonly #commitObservers = new Set<() => void>();

  constructor(
    lc: LogContext,
    db: Database,
    schema: Schema,
    logConfig?: LogConfig | undefined,
  ) {
    this.#lc = lc.withContext('class', 'QueryDelegateImpl');
    this.#db = db;
    this.#schema = schema;
    this.#sources = new Map();
    this.#logConfig = logConfig ?? {
      format: 'text',
      ivmSampling: 0,
      level: 'info',
      slowHydrateThreshold: 0,
      slowRowThreshold: 0,
    };
  }

  getSource(tableName: string): Source {
    let source = this.#sources.get(tableName);
    if (source) {
      return source;
    }

    const tableSchema = this.#schema.tables[tableName];

    source = new TableSource(
      this.#lc,
      this.#logConfig,
      'query.test.ts',
      this.#db,
      tableName,
      tableSchema.columns,
      tableSchema.primaryKey,
    );

    this.#sources.set(tableName, source);
    return source;
  }

  createStorage() {
    return new MemoryStorage();
  }
  decorateInput(input: Input): Input {
    return input;
  }
  decorateFilterInput(input: FilterInput): FilterInput {
    return input;
  }
  addServerQuery() {
    return () => {};
  }
  addCustomQuery() {
    return () => {};
  }
  updateServerQuery() {}
  updateCustomQuery() {}
  onQueryMaterialized() {}
  onTransactionCommit(cb: CommitListener) {
    this.#commitObservers.add(cb);
    return () => {
      this.#commitObservers.delete(cb);
    };
  }
  batchViewUpdates<T>(applyViewUpdates: () => T): T {
    const ret = applyViewUpdates();
    for (const observer of this.#commitObservers) {
      observer();
    }
    return ret;
  }
  assertValidRunOptions() {}
}
