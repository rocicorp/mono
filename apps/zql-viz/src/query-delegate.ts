import type {Schema} from '../../../packages/zero-types/src/schema.ts';
import type {FilterInput} from '../../../packages/zql/src/ivm/filter-operators.ts';
import {MemorySource} from '../../../packages/zql/src/ivm/memory-source.ts';
import type {Input, InputBase} from '../../../packages/zql/src/ivm/operator.ts';
import type {SourceInput} from '../../../packages/zql/src/ivm/source.ts';
import {QueryDelegateBase} from '../../../packages/zql/src/query/query-delegate-base.ts';
import type {Edge, Graph} from './types.ts';

export class VizDelegate extends QueryDelegateBase<undefined> {
  readonly #sources: Map<string, MemorySource> = new Map();
  readonly #schema: Schema;

  readonly #nodeIds: Map<
    InputBase,
    {
      id: number;
      type: string;
      name: string;
    }
  > = new Map();
  readonly #edges: Edge[] = [];
  #nodeIdCounter = 0;
  readonly defaultQueryComplete: boolean = true;

  readonly applyFiltersAnyway = true;

  constructor(schema: Schema) {
    super(undefined);
    this.#schema = schema;
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
