import type {LogContext} from '@rocicorp/logger';
import type {LogConfig} from '../../otel/src/log-options.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import type {Source} from '../../zql/src/ivm/source.ts';
import {QueryDelegateBase} from '../../zql/src/query/query-delegate-base.ts';
import type {CommitListener} from '../../zql/src/query/query-delegate.ts';
import type {Database} from './db.ts';
import {TableSource} from './table-source.ts';

export class QueryDelegateImpl extends QueryDelegateBase {
  readonly #lc: LogContext;
  readonly #db: Database;
  readonly #schema: Schema;
  readonly #sources: Map<string, Source> = new Map();
  readonly #logConfig: LogConfig;
  readonly defaultQueryComplete = true;
  readonly #commitObservers = new Set<() => void>();

  constructor(
    lc: LogContext,
    db: Database,
    schema: Schema,
    logConfig?: LogConfig,
  ) {
    super();
    this.#lc = lc.withContext('class', 'QueryDelegateImpl');
    this.#db = db;
    this.#schema = schema;
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
      this.#db,
      tableName,
      tableSchema.columns,
      tableSchema.primaryKey,
    );

    this.#sources.set(tableName, source);
    return source;
  }

  onTransactionCommit(cb: CommitListener) {
    this.#commitObservers.add(cb);
    return () => {
      this.#commitObservers.delete(cb);
    };
  }
  override batchViewUpdates<T>(applyViewUpdates: () => T): T {
    const ret = applyViewUpdates();
    for (const observer of this.#commitObservers) {
      observer();
    }
    return ret;
  }
}
