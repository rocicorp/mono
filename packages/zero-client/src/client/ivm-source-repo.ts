import {MemorySource} from '../../../zql/src/ivm/memory-source.ts';
import type {TableSchema} from '../../../zero-schema/src/table-schema.ts';
import {wrapIterable} from '../../../shared/src/iterables.ts';
import {
  mustGetHeadHash,
  type Read,
  type Store,
} from '../../../replicache/src/dag/store.ts';
import {withRead} from '../../../replicache/src/with-transactions.ts';
import type {Hash} from '../../../replicache/src/hash.ts';
import * as FormatVersion from '../../../replicache/src/format-version-enum.ts';
import {ENTITIES_KEY_PREFIX, sourceNameFromKey} from './keys.ts';
import {must} from '../../../shared/src/must.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Diff} from '../../../replicache/src/sync/patch.ts';
import {readFromHash} from '../../../replicache/src/db/read.ts';
import type {DetailedReason} from '../../../replicache/src/transactions.ts';
import type {RepTxZeroData} from './custom.ts';
import {diff} from '../../../replicache/src/sync/diff.ts';
import type {MaybePromise} from '../../../shared/src/types.ts';
import {Lock} from '@rocicorp/lock';
import {assert} from '../../../shared/src/asserts.ts';
import {SYNC_HEAD_NAME} from '../../../replicache/src/sync/sync-head-name.ts';
import type {InternalDiff} from '../../../replicache/src/btree/node.ts';

/**
 * Provides handles to IVM sources at different heads.
 *
 * - sync always matches the server snapshot
 * - main is the client's current view of the data
 */
export class IVMSourceRepo {
  readonly #main: IVMSourceBranch;
  readonly #tables: Record<string, TableSchema>;

  // We have a lock here because `refresh` and `maybePullEnd` could both
  // be running simultaneously. We don't want to create the sync head twice.
  readonly #initSyncHeadLock: Lock;

  /**
   * Sync is lazily created when the first response from the server is received.
   */
  #sync: IVMSyncBranch | undefined;

  constructor(tables: Record<string, TableSchema>) {
    this.#main = new IVMSourceBranch(tables, undefined);
    this.#tables = tables;
    this.#initSyncHeadLock = new Lock();
  }

  get main() {
    return this.#main;
  }

  /**
   * When replicache starts a rebase or mutation, it calls
   * this method to create the IVM sources at the expected head.
   */
  getSourcesForTransaction(
    reason: DetailedReason,
    expectedHead:
      | {
          store: Store;
          hash: Hash;
        }
      | undefined,
  ): MaybePromise<RepTxZeroData> {
    switch (reason) {
      case 'initial': {
        assert(
          expectedHead === undefined,
          'initial must ron on main not on a custom head',
        );
        // Mutators read from main and do not write to main for `initial` mutations.
        // Main is updated via `experimentalWatch` between each mutation.
        // There is no concept of running many custom mutators in the same transaction at the moment.
        // If that changes we'll have to revisit.
        return {
          read: this.#main,
          write: undefined,
        };
      }
      case 'persist':
      case 'pullEnd':
      case 'refresh': {
        assert(
          expectedHead !== undefined,
          () => 'expectedHead must be specified for ' + reason,
        );
        if (this.#sync === undefined) {
          return this.#createSourcesForHead(expectedHead);
        }

        if (this.#sync.hash === expectedHead.hash) {
          // If the hashes are the same, the sync head is already up to date.
          const fork = this.#sync.fork();
          return {read: fork, write: fork};
        }

        // On refresh the hashes may not match.
        // This is because IDB will have mutations included in it
        // that are not yet in the sync head.
        assert(reason === 'refresh');
        return this.#createSourcesForHead(expectedHead);
      }
    }
  }

  async #createSourcesForHead({
    store,
    hash,
    read,
  }: {
    store: Store;
    hash: Hash;
    read?: Read;
  }): Promise<RepTxZeroData> {
    // If the sync head does not exist yet, create it.
    // It will be fast-forwarded to the desired head.
    if (this.#sync === undefined) {
      await this.#initSyncHead(store, undefined);
    }
    assert(this.#sync !== undefined);
    // fork sync now so it cannot change while
    // we await the diffs.
    const fork = this.#sync.fork();

    const readFn = (dagRead: Read) =>
      diff(
        fork.hash,
        hash,
        dagRead,
        {
          shouldComputeDiffs: () => true,
          shouldComputeDiffsForIndex(_name) {
            return false;
          },
        },
        FormatVersion.Latest,
      );
    const diffsFromSync =
      read === undefined ? await withRead(store, readFn) : await readFn(read);

    const diffs = diffsFromSync.get('');
    if (diffs === undefined) {
      return {read: fork, write: fork};
    }

    applyDiffs(diffs, fork);
    fork.hash = hash;
    return {read: fork, write: fork};
  }

  async #initSyncHead(store: Store, syncHeadHash: Hash | undefined) {
    await this.#initSyncHeadLock.withLock(async () => {
      if (this.#sync !== undefined) {
        // sync head was created by someone else while we were waiting for the lock.
        return;
      }

      await withRead(store, async dagRead => {
        if (syncHeadHash === undefined) {
          syncHeadHash = await mustGetHeadHash(SYNC_HEAD_NAME, dagRead);
        }
        const syncSources = new IVMSyncBranch(this.#tables, syncHeadHash);
        const read = await readFromHash(
          syncHeadHash,
          dagRead,
          FormatVersion.Latest,
        );
        for await (const entry of read.map.scan(ENTITIES_KEY_PREFIX)) {
          if (!entry[0].startsWith(ENTITIES_KEY_PREFIX)) {
            break;
          }
          const name = sourceNameFromKey(entry[0]);
          const source = must(syncSources.getSource(name));
          source.push({
            type: 'add',
            row: entry[1] as Row,
          });
        }
        this.#sync = syncSources;
      });
    });
  }

  advanceSyncHead = async (
    store: Store,
    syncHeadHash: Hash,
    patches: readonly Diff[],
  ): Promise<void> => {
    /**
     * The sync head may not exist yet as we do not create it on construction of Zero.
     * One reason it is not created eagerly is that the `main` head must exist immediately
     * on startup of Zero since a user can construct queries without awaiting. E.g.,
     *
     * ```ts
     * const z = new Zero();
     * z.query.issue...
     * ```
     *
     * Since the main `IVM Sources` must exist immediately on construction of Zero,
     * we cannot wait for `sync` to be populated then forked off into `main`.
     */
    if (this.#sync === undefined) {
      await this.#initSyncHead(store, syncHeadHash);
    }
    assert(this.#sync !== undefined);

    if (this.#sync.hash === syncHeadHash) {
      // If the hashes are the same, the sync head is already up to date.
      return;
    }

    // Sync head was behind. Advance it via the provided diffs.
    applyDiffs(patches, this.#sync);
    this.#sync.hash = syncHeadHash;
  };
}

function applyDiffs(
  patches: readonly Diff[] | InternalDiff,
  branch: IVMSyncBranch,
) {
  for (const patch of patches) {
    if (patch.op === 'clear') {
      branch.clear();
      continue;
    }

    const {key} = patch;
    if (!key.startsWith(ENTITIES_KEY_PREFIX)) {
      continue;
    }
    const name = sourceNameFromKey(key);
    const source = must(branch.getSource(name));
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

export class IVMSourceBranch {
  readonly #sources: Map<string, MemorySource | undefined>;
  readonly #tables: Record<string, TableSchema>;
  hash: Hash | undefined;

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
   * IVM branches are forked when we need to rebase mutations.
   * The mutations modify the fork rather than original branch.
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

class IVMSyncBranch extends IVMSourceBranch {
  readonly #sources: Map<string, MemorySource | undefined>;
  readonly #tables: Record<string, TableSchema>;
  hash: Hash;

  constructor(
    tables: Record<string, TableSchema>,
    hash: Hash,
    sources: Map<string, MemorySource | undefined> = new Map(),
  ) {
    super(tables, sources);
    this.hash = hash;
    this.#sources = sources;
    this.#tables = tables;
  }

  fork() {
    return new IVMSyncBranch(
      this.#tables,
      this.hash,
      new Map(
        wrapIterable(this.#sources.entries()).map(([name, source]) => [
          name,
          source?.fork(),
        ]),
      ),
    );
  }
}
