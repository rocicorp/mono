import type {Schema} from '../../../packages/zero-schema/src/builder/schema-builder.ts';
import type {FilterInput} from '../../../packages/zql/src/ivm/filter-operators.ts';
import {MemorySource} from '../../../packages/zql/src/ivm/memory-source.ts';
import {MemoryStorage} from '../../../packages/zql/src/ivm/memory-storage.ts';
import type {
  Input,
  InputBase,
  Storage,
} from '../../../packages/zql/src/ivm/operator.ts';
import type {SourceInput} from '../../../packages/zql/src/ivm/source.ts';
import type {ViewFactory} from '../../../packages/zql/src/ivm/view.ts';
import type {QueryDelegate} from '../../../packages/zql/src/query/query-delegate.ts';
import {
  materializeImpl,
  preloadImpl,
  runImpl,
} from '../../../packages/zql/src/query/query-impl.ts';
import {queryWithContext} from '../../../packages/zql/src/query/query-internals.ts';
import type {
  AnyQuery,
  HumanReadable,
  MaterializeOptions,
  PreloadOptions,
  Query,
  RunOptions,
} from '../../../packages/zql/src/query/query.ts';
import type {Edge, Graph} from './types.ts';

export class VizDelegate implements QueryDelegate<undefined> {
  readonly #sources: Map<string, MemorySource>;
  readonly #schema: Schema;

  readonly #nodeIds: Map<
    InputBase,
    {
      id: number;
      type: string;
      name: string;
    }
  >;
  readonly #edges: Edge[];
  #nodeIdCounter = 0;
  readonly defaultQueryComplete: boolean = true;

  readonly applyFiltersAnyway = true;

  constructor(schema: Schema) {
    this.#sources = new Map();
    this.#schema = schema;
    this.#nodeIds = new Map();
    this.#edges = [];
  }

  getGraph(): Graph {
    return {
      nodes: Array.from(this.#nodeIds.values()),
      edges: this.#edges,
    };
  }

  getSource(name: string) {
    const existing = this.#sources.get(name);
    if (existing) {
      return existing;
    }

    const tableSchema = this.#schema.tables[name];
    const newSource = new MemorySource(
      name,
      tableSchema.columns,
      tableSchema.primaryKey,
    );
    this.#sources.set(name, newSource);
    return newSource;
  }

  createStorage(): Storage {
    return new MemoryStorage();
  }

  decorateInput(input: Input, name: string): Input {
    this.#getNode(input, name);
    return input;
  }

  addEdge(source: InputBase, dest: InputBase): void {
    const sourceNode = this.#getNode(source);
    const destNode = this.#getNode(dest);
    this.#edges.push({source: sourceNode.id, dest: destNode.id});
  }

  decorateSourceInput(input: SourceInput, queryID: string): Input {
    const node = this.#getNode(input, queryID);
    node.type = 'SourceInput';
    return input;
  }

  decorateFilterInput(input: FilterInput, name: string): FilterInput {
    this.#getNode(input, name);
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
  onTransactionCommit() {
    return () => {};
  }
  batchViewUpdates<T>(applyViewUpdates: () => T): T {
    return applyViewUpdates();
  }
  assertValidRunOptions() {}
  flushQueryChanges() {}
  addMetric() {}

  materialize<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
    TContext,
    T,
  >(
    query: Query<TSchema, TTable, TReturn, TContext>,
    factory?: ViewFactory<TSchema, TTable, TReturn, TContext, T>,
    options?: MaterializeOptions,
  ): T {
    return materializeImpl(query, this, factory, options);
  }

  run<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
    TContext,
  >(
    query: Query<TSchema, TTable, TReturn, TContext>,
    options?: RunOptions,
  ): Promise<HumanReadable<TReturn>> {
    return runImpl(query, this, options);
  }

  preload<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
    TContext,
  >(
    query: Query<TSchema, TTable, TReturn, TContext>,
    options?: PreloadOptions,
  ): {
    cleanup: () => void;
    complete: Promise<void>;
  } {
    return preloadImpl(query, this, options);
  }

  withContext(query: AnyQuery) {
    return queryWithContext(query, undefined);
  }

  #getNode(input: InputBase, name?: string) {
    const existing = this.#nodeIds.get(input);
    if (existing) {
      if (name) {
        existing.name = name;
      }
      return existing;
    }

    const newNode = {
      id: this.#nodeIdCounter++,
      name: name ?? `Node ${this.#nodeIdCounter}`,
      type: input.constructor.name,
    };
    this.#nodeIds.set(input, newNode);
    return newNode;
  }
}
