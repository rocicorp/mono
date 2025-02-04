import {MemorySource} from '../../../zql/src/ivm/memory-source.ts';
import type {TableSchema} from '../../../zero-schema/src/table-schema.ts';
import {wrapIterable} from '../../../shared/src/iterables.ts';
import type {Store} from '../../../replicache/src/dag/store.ts';
import {withRead} from '../../../replicache/src/with-transactions.ts';
import type {Hash} from '../../../replicache/src/hash.ts';
import {BTreeRead} from '../../../replicache/src/btree/read.ts';
import * as FormatVersion from '../../../replicache/src/format-version-enum.ts';
import {ENTITIES_KEY_PREFIX} from './keys.ts';
import {must} from '../../../shared/src/must.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Diff} from '../../../replicache/src/sync/patch.ts';

/**
 * Provides handles to IVM sources at different heads.
 *
 * - sync always matches the server snapshot
 * - main is the client's current view of the data
 */
export class IVMSourceRepo {
  readonly #main: IVMSourceBranch;
  readonly #tables: Record<string, TableSchema>;
  // sync is undefined until the first pull
  #sync: IVMSourceBranch | undefined;
  // rebase is set to a fork of sync prior to a rebase
  // rebased mutations are applied to this branch
  #rebase: IVMSourceBranch | undefined;

  constructor(tables: Record<string, TableSchema>) {
    this.#main = new IVMSourceBranch(tables);
    this.#tables = tables;
  }

  get main() {
    return this.#main;
  }

  get rebase() {
    return must(this.#rebase, 'rebase branch does not exist!');
  }

  advanceSyncHead = async (
    store: Store,
    syncHeadHash: Hash,
    patches: readonly Diff[],
  ): Promise<void> => {
    /**
     * The sync head may not exist yet as we do not create it on construction of Zero.
     * One reason is that the `main` head must exist immediately on startup of Zero since
     * a user can immediately construct queries on Zero without awaiting. E.g.,
     *
     * ```ts
     * const z = new Zero();
     * z.query.issue...
     * ```
     *
     * This means that the original plan of:
     * 1. Create the sync head
     * 2. Fork sync head
     * 3. Create main head via applying diffs to the forked sync head
     *
     * Does not work since it would require the user to `await` before
     * they can construct a query.
     *
     * Instead, we create an empty `main` head immediately on construction of Zero
     * which is later filled by `experimentalWatch`.
     *
     * To do less work on startup, we defer sync head creation until the
     * first response from the server.
     *
     */
    if (this.#sync === undefined) {
      await withRead(store, async dagRead => {
        const syncSources = new IVMSourceBranch(this.#tables);
        for await (const entry of new BTreeRead(
          dagRead,
          FormatVersion.Latest,
          syncHeadHash,
        ).scan(ENTITIES_KEY_PREFIX)) {
          const name = nameFromKey(entry[0]);
          const source = must(syncSources.getSource(name));
          source.push({
            type: 'add',
            row: entry[1] as Row,
          });
        }
        this.#sync = syncSources;
      });
    } else {
      // sync head already exists so we advance it from the array of diffs.
      for (const patch of patches) {
        if (patch.op === 'clear') {
          this.#sync.clear();
          continue;
        }

        const {key} = patch;
        const name = nameFromKey(key);
        const source = must(this.#sync.getSource(name));
        switch (patch.op) {
          case 'del':
            source.push({
              type: 'remove',
              row: patch.oldValue as Row,
            });
            break;
          case 'add':
            source.push({
              type: 'add',
              row: patch.newValue as Row,
            });
            break;
          case 'change':
            source.push({
              type: 'edit',
              row: patch.newValue as Row,
              oldRow: patch.oldValue as Row,
            });
            break;
        }
      }
    }

    // Set the branch which can be used for rebasing optimistic mutations.
    this.#rebase = must(this.#sync).fork();
  };
}

export function nameFromKey(key: string): string {
  const slash = key.indexOf('/', ENTITIES_KEY_PREFIX.length);
  return key.slice(ENTITIES_KEY_PREFIX.length, slash);
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

  clear() {
    this.#sources.clear();
  }

  /**
   * Creates a new IVMSourceBranch that is a copy of the current one.
   * This is a cheap operation since the b-trees are shared until a write is performed
   * and then only the modified nodes are copied.
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
