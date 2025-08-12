import {assert} from '../../../shared/src/asserts.ts';
import type {JSONObject} from '../../../shared/src/json.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {FilterInput} from '../ivm/filter-operators.ts';
import {MemoryStorage} from '../ivm/memory-storage.ts';
import type {Input, Storage} from '../ivm/operator.ts';
import {FilterSnitch, Snitch, type SnitchMessage} from '../ivm/snitch.ts';
import type {Source, SourceInput} from '../ivm/source.ts';
import type {BuilderDelegate} from './builder.ts';

export class TestBuilderDelegate implements BuilderDelegate {
  readonly #sources: Readonly<Record<string, Source>>;
  readonly #storage: Record<string, MemoryStorage> = {};
  readonly #shouldLog: boolean;
  readonly #log: SnitchMessage[] = [];

  constructor(
    sources: Readonly<Record<string, Source>>,
    shouldLog?: boolean | undefined,
  ) {
    this.#sources = sources;
    this.#shouldLog = !!shouldLog;
  }

  getSource(tableName: string): Source | undefined {
    assert(
      Object.hasOwn(this.#sources, tableName),
      `Missing source ${tableName}`,
    );
    return this.#sources[tableName];
  }

  mapAst(ast: AST): AST {
    return ast;
  }

  createStorage(name: string): Storage {
    assert(!Object.hasOwn(this.#storage, name));
    const storage = new MemoryStorage();
    this.#storage[name] = storage;
    return storage;
  }

  decorateInput(input: Input, name: string): Input {
    if (!this.#shouldLog) {
      return input;
    }
    return new Snitch(input, name, this.#log);
  }

  decorateFilterInput(input: FilterInput, name: string): FilterInput {
    if (!this.#shouldLog) {
      return input;
    }
    return new FilterSnitch(input, name, this.#log);
  }

  decorateSourceInput(input: SourceInput): Input {
    return input;
  }

  clearLog() {
    if (this.#log) {
      this.#log.length = 0;
    }
  }

  get log() {
    return this.#log;
  }

  get clonedStorage(): Record<string, JSONObject> {
    const cloned: Record<string, JSONObject> = {};
    for (const [name, s] of Object.entries(this.#storage)) {
      cloned[name] = s.cloneData();
    }
    return cloned;
  }
}
