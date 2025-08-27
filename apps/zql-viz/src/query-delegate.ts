import type {Schema} from '@rocicorp/zero';
import {MemorySource} from '../../../packages/zql/src/ivm/memory-source.ts';
import type {QueryDelegate} from '../../../packages/zql/src/query/query-delegate.ts';
import type {
  Input,
  InputBase,
  Storage,
} from '../../../packages/zql/src/ivm/operator.ts';
import {MemoryStorage} from '../../../packages/zero/out/zql/src/ivm/memory-storage';
import type {Edge, Graph} from './types.ts';
import type {SourceInput} from '../../../packages/zql/src/ivm/source.ts';
import type {FilterInput} from '../../../packages/zql/src/ivm/filter-operators.ts';

export class VizDelegate implements QueryDelegate {
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

  #getNode(input: InputBase, name?: string | undefined) {
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
