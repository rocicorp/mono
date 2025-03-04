import {MemorySource} from '../../../zql/src/ivm/memory-source.ts';
import type {TableSchema} from '../../../zero-schema/src/table-schema.ts';
import {wrapIterable} from '../../../shared/src/iterables.ts';
import {type Read, type Store} from '../../../replicache/src/dag/store.ts';
import {withRead} from '../../../replicache/src/with-transactions.ts';
import type {Hash} from '../../../replicache/src/hash.ts';
import * as FormatVersion from '../../../replicache/src/format-version-enum.ts';
import {ENTITIES_KEY_PREFIX, sourceNameFromKey} from './keys.ts';
import {must} from '../../../shared/src/must.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {DetailedReason} from '../../../replicache/src/transactions.ts';
import type {RepTxZeroData} from './custom.ts';
import {diff} from '../../../replicache/src/sync/diff.ts';
import {assert} from '../../../shared/src/asserts.ts';
import type {InternalDiff} from '../../../replicache/src/btree/node.ts';
import {resolver} from '@rocicorp/resolver';

/**
 *
 */
export class IVMSourceRepo {
  readonly #main: IVMSourceBranch;
  readonly #mainInitializedPromise: Promise<boolean>;
  #mainInitialized: boolean;
  readonly #resolveMainInitialized: (value: boolean) => void;

  constructor(tables: Record<string, TableSchema>) {
    this.#main = new IVMSourceBranch(tables, undefined);
    const {promise, resolve} = resolver<boolean>();
    this.#mainInitializedPromise = promise;
    this.#mainInitialized = false;
    this.#resolveMainInitialized = resolve;
  }

  get main() {
    return this.#main;
  }

  /**
   * Gets the IVM sources for the specific transaction reason:
   * initial, pullEnd, persist, or refresh.
   */
  async getSourcesForTransaction(
    reason: DetailedReason,
    store: Store,
    expectedHead: Hash,
    desiredHead: Hash,
  ): Promise<RepTxZeroData> {
    switch (reason) {
      case 'initial': {
        assert(
          expectedHead === undefined,
          'expected head should be undefined for initial',
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
        if (this.#mainInitialized === false) {
          await this.#mainInitializedPromise;
        }

        const fork = this.#main.fork();
        assert(
          expectedHead === fork.hash && expectedHead !== undefined,
          () =>
            `expected head must be defined for ${reason} and match the main head. Got: ${expectedHead}, expected: ${
              this.#main.hash
            }`,
        );

        if (fork.hash === desiredHead) {
          return {read: fork, write: fork};
        }

        return this.#patchSourceForHead(desiredHead, store, fork);
      }
    }
  }

  async #patchSourceForHead(
    desiredHead: Hash,
    store: Store,
    fork: IVMSourceBranch,
  ): Promise<RepTxZeroData> {
    const diffs = await computeDiffs(
      must(fork.hash),
      desiredHead,
      store,
      undefined,
    );
    if (!diffs) {
      return {read: fork, write: fork};
    }
    applyDiffs(diffs, fork);
    return {read: fork, write: fork};
  }
}

async function computeDiffs(
  startHash: Hash,
  endHash: Hash,
  store: Store,
  read: Read | undefined,
): Promise<InternalDiff | undefined> {
  const readFn = (dagRead: Read) =>
    diff(
      startHash,
      endHash,
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

  return diffsFromSync.get('');
}

function applyDiffs(patches: InternalDiff, branch: IVMSourceBranch) {
  for (const patch of patches) {
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
    hash: Hash | undefined,
    sources: Map<string, MemorySource | undefined> = new Map(),
  ) {
    this.#tables = tables;
    this.#sources = sources;
    this.hash = hash;
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
