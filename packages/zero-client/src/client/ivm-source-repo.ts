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
import type {MaybePromise} from '../../../shared/src/types.ts';

/**
 *
 */
export class IVMSourceRepo {
  readonly #main: IVMSourceBranch;

  constructor(tables: Record<string, TableSchema>) {
    this.#main = new IVMSourceBranch(tables);
  }

  get main() {
    return this.#main;
  }

  /**
   * Gets the IVM sources for the specific transaction reason:
   * initial, pullEnd, persist, or refresh.
   */
  getSourcesForTransaction(
    reason: DetailedReason,
    store: Store,
    expectedHead: Hash,
    desiredHead: Hash,
    read: Read | undefined,
  ): MaybePromise<RepTxZeroData> {
    const fork = this.#main.fork();
    assert(
      expectedHead === fork.hash,
      () =>
        `expected head must match the main head. Got: ${expectedHead}, expected: ${fork.hash} for reason: ${reason}`,
    );
    if (fork.hash === desiredHead) {
      return fork;
    }

    return patchSource(desiredHead, store, fork, read);
  }
}

async function patchSource(
  desiredHead: Hash,
  store: Store,
  fork: IVMSourceBranch,
  read: Read | undefined,
) {
  const diffs = await computeDiffs(must(fork.hash), desiredHead, store, read);
  if (!diffs) {
    return fork;
  }
  applyDiffs(diffs, fork);
  return fork;
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
  // TODO: binary search for the first patch that is prefixed with ENTITIES_KEY_PREFIX
  // see code in subscriptions.ts
  // TODO: break as soon as one does not start with that prefix.
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
  readonly ready: Promise<boolean>;
  readonly #resolveReady: (value: boolean) => void;
  hash: Hash | undefined;

  constructor(
    tables: Record<string, TableSchema>,
    hash?: Hash | undefined,
    sources: Map<string, MemorySource | undefined> = new Map(),
  ) {
    this.#tables = tables;
    this.#sources = sources;
    this.hash = hash;
    const {promise, resolve} = resolver<boolean>();
    this.ready = promise;
    this.#resolveReady = resolve;
  }

  resolveReady() {
    assert(this.hash !== undefined, 'hash must be set before resolving');
    this.#resolveReady(true);
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
