import type {
  InternalDiff,
  InternalDiffOperation,
  NoIndexDiff,
} from '../../../replicache/src/btree/node.ts';
import type {LazyStore} from '../../../replicache/src/dag/lazy-store.ts';
import {type Read, type Store} from '../../../replicache/src/dag/store.ts';
import {readFromHash} from '../../../replicache/src/db/read.ts';
import * as FormatVersion from '../../../replicache/src/format-version-enum.ts';
import type {Hash} from '../../../replicache/src/hash.ts';
import type {ZeroReadOptions} from '../../../replicache/src/replicache-options.ts';
import {diffBinarySearch} from '../../../replicache/src/subscriptions.ts';
import type {DiffsMap} from '../../../replicache/src/sync/diff.ts';
import {diff} from '../../../replicache/src/sync/diff.ts';
import {using, withRead} from '../../../replicache/src/with-transactions.ts';
import {assert} from '../../../shared/src/asserts.ts';
import {wrapIterable} from '../../../shared/src/iterables.ts';
import {must} from '../../../shared/src/must.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {TableSchema} from '../../../zero-schema/src/table-schema.ts';
import {MemorySource} from '../../../zql/src/ivm/memory-source.ts';
import {consume} from '../../../zql/src/ivm/stream.ts';
import {ENTITIES_KEY_PREFIX, sourceNameFromKey} from './keys.ts';

/**
 * Replicache needs to rebase mutations onto different
 * commits of it's b-tree. These mutations can have reads
 * in them and those reads must be run against the IVM sources.
 *
 * To ensure the reads get the correct state, the IVM
 * sources need to reflect the state of the commit
 * being rebased onto. `IVMSourceBranch` allows us to:
 * 1. fork the IVM sources
 * 2. patch them up to match the desired head
 * 3. run the reads against the forked sources
 *
 * (2) is expected to be a cheap operation as there should only
 * ever be a few outstanding diffs to apply given Zero is meant
 * to be run in a connected state.
 */
export class IVMSourceBranch {
  readonly #sources: Map<string, MemorySource | undefined>;
  readonly #tables: Record<string, TableSchema>;
  #debugName: string | undefined;
  hash: Hash | undefined;

  constructor(
    tables: Record<string, TableSchema>,
    hash?: Hash,
    sources?: Map<string, MemorySource | undefined>,
    debugName?: string | undefined,
  ) {
    this.#tables = tables;
    this.#sources = sources ?? new Map();
    this.hash = hash;
    this.#debugName = debugName;
  }

  setDebugName(name: string) {
    this.#debugName = name;
  }

  getSource(name: string): MemorySource | undefined {
    if (this.#sources.has(name)) {
      return this.#sources.get(name);
    }

    const schema = this.#tables[name];
    const source = schema
      ? new MemorySource(
          name,
          schema.columns,
          schema.primaryKey,
          undefined,
          this.#debugName,
        )
      : undefined;
    this.#sources.set(name, source);
    return source;
  }

  clear() {
    this.#sources.clear();
  }

  /**
   * Mutates the current branch, advancing it to the new head
   * by applying the given diffs.
   */
  advance(
    expectedHead: Hash | undefined,
    newHead: Hash,
    diffs: NoIndexDiff,
    reason: ApplyDiffsReason = 'poke',
  ) {
    assert(
      this.hash === expectedHead,
      () =>
        `Expected head must match the main head. Got: ${this.hash}, expected: ${expectedHead}`,
    );

    applyDiffs(diffs, this, reason);
    this.hash = newHead;
  }

  /**
   * Fork the branch and patch it up to match the desired head.
   */
  async forkToHead(
    store: LazyStore,
    desiredHead: Hash,
    readOptions?: ZeroReadOptions,
  ): Promise<IVMSourceBranch> {
    const fork = this.fork();

    if (fork.hash === desiredHead) {
      return fork;
    }

    await patchBranch(desiredHead, store, fork, readOptions);
    fork.hash = desiredHead;
    return fork;
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
      this.#debugName ? `${this.#debugName}:fork` : undefined,
    );
  }
}

export async function initFromStore(
  branch: IVMSourceBranch,
  hash: Hash,
  store: Store,
) {
  const diffs: InternalDiffOperation[] = [];
  await withRead(store, async dagRead => {
    const read = await readFromHash(hash, dagRead, FormatVersion.Latest);
    for await (const entry of read.map.scan(ENTITIES_KEY_PREFIX)) {
      if (!entry[0].startsWith(ENTITIES_KEY_PREFIX)) {
        break;
      }
      diffs.push({
        op: 'add',
        key: entry[0],
        newValue: entry[1],
      });
    }
  });

  branch.advance(undefined, hash, diffs, 'init');
}

async function patchBranch(
  desiredHead: Hash,
  store: LazyStore,
  fork: IVMSourceBranch,
  readOptions: ZeroReadOptions | undefined,
) {
  const diffs = await computeDiffs(
    must(fork.hash),
    desiredHead,
    store,
    readOptions,
  );
  if (!diffs) {
    return;
  }
  applyDiffs(diffs, fork, 'rebase');
}

async function computeDiffs(
  startHash: Hash,
  endHash: Hash,
  store: LazyStore,
  readOptions: ZeroReadOptions | undefined,
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

  let diffs: DiffsMap;
  if (readOptions?.openLazySourceRead) {
    diffs = await using(store.read(readOptions.openLazySourceRead), readFn);
  } else if (readOptions?.openLazyRead) {
    diffs = await readFn(readOptions.openLazyRead);
  } else {
    diffs = await withRead(store, readFn);
  }

  return diffs.get('');
}

type ApplyDiffsReason = 'poke' | 'rebase' | 'init' | 'refresh';

function applyDiffs(
  diffs: NoIndexDiff,
  branch: IVMSourceBranch,
  reason: ApplyDiffsReason,
) {
  for (
    let i = diffBinarySearch(diffs, ENTITIES_KEY_PREFIX, diff => diff.key);
    i < diffs.length;
    i++
  ) {
    const diff = diffs[i];
    const {key} = diff;
    if (!key.startsWith(ENTITIES_KEY_PREFIX)) {
      break;
    }
    const name = sourceNameFromKey(key);
    const source = must(branch.getSource(name));
    const options = {reason};
    switch (diff.op) {
      case 'del':
        consume(
          source.push(
            {
              type: 'remove',
              row: diff.oldValue as Row,
            },
            options,
          ),
        );
        break;
      case 'add':
        consume(
          source.push(
            {
              type: 'add',
              row: diff.newValue as Row,
            },
            options,
          ),
        );
        break;
      case 'change':
        consume(
          source.push(
            {
              type: 'edit',
              row: diff.newValue as Row,
              oldRow: diff.oldValue as Row,
            },
            options,
          ),
        );
        break;
    }
  }
}
