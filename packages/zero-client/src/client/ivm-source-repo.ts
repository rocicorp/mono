import {MemorySource} from '../../../zql/src/ivm/memory-source.ts';
import type {TableSchema} from '../../../zero-schema/src/table-schema.ts';
import {wrapIterable} from '../../../shared/src/iterables.ts';

/**
 * Provides handles to IVM sources at different heads.
 *
 * - sync always matches the server snapshot
 * - main is the client's current view of the data
 */
export class IVMSourceRepo {
  readonly #main: IVMSourceBranch;
  sync: IVMSourceBranch;

  constructor(main: IVMSourceBranch, sync: IVMSourceBranch) {
    this.#main = main;
    this.sync = sync;
  }

  get main() {
    return this.#main;
  }
}

export class IVMSourceBranch {
  readonly #sources: Map<string, MemorySource | undefined>;
  readonly #tables: Record<string, TableSchema>;

  constructor(
    tables: Record<string, TableSchema>,
    sources: Map<string, MemorySource | undefined> = new Map(),
  ) {
    this.#tables = tables;
    this.#sources = sources;
  }

  getSource(name: string): MemorySource | undefined {
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

  /**
   * Creates a new IVMSourceBranch that is a copy of the current one.
   *
   * This is used when:
   * 1. We need to rebase a change. We fork the `sync` branch and run the mutations against the fork.
   * 2. We need to create `main` at startup.
   * 3. We need to create a new `sync` head because we got a new server snapshot.
   *    The old `sync` head is forked and the new server snapshot is applied to the fork.
   */
  fork() {
    return new IVMSourceBranch(
      this.#tables,
      new Map(
        wrapIterable(this.#sources.entries()).map(([name, source]) => [
          name,
          source?.fork(),
        ]),
      ),
    );
  }
}
