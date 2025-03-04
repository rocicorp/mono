import type {
  DiffOperation,
  InternalDiff,
} from '../../../replicache/src/btree/node.ts';
import type {Store} from '../../../replicache/src/dag/store.ts';
import {readFromHash} from '../../../replicache/src/db/read.ts';
import type {Hash} from '../../../replicache/src/hash.ts';
import {withRead} from '../../../replicache/src/with-transactions.ts';
import type {ZeroContext} from './context.ts';
import * as FormatVersion from '../../../replicache/src/format-version-enum.ts';
import type {IVMSourceBranch, IVMSourceRepo} from './ivm-source-repo.ts';
import {ENTITIES_KEY_PREFIX} from './keys.ts';
import {must} from '../../../shared/src/must.ts';
import type {DetailedReason} from '../../../replicache/src/transactions.ts';

export class ZeroRep {
  readonly #context: ZeroContext;
  readonly #ivmSources: IVMSourceRepo;
  #store: Store | undefined;

  constructor(context: ZeroContext, ivmSources: IVMSourceRepo) {
    this.#context = context;
    this.#ivmSources = ivmSources;
  }

  async init(hash: Hash, store: Store) {
    const diffs: DiffOperation<string>[] = [];
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
    this.#store = store;

    this.#context.processChanges(diffs, () => {
      this.#ivmSources.main.hash = hash;
      this.#ivmSources.main.resolveReady();
    });
  }

  async getTxData(
    reason: DetailedReason,
    expectedHead: Hash,
    desiredHead: Hash,
  ): Promise<IVMSourceBranch> {
    await this.#ivmSources.main.ready;
    return this.#ivmSources.getSourcesForTransaction(
      reason,
      must(this.#store),
      expectedHead,
      desiredHead,
    );
  }

  async advance(hash: Hash, changes: InternalDiff): Promise<void> {
    await this.#ivmSources.main.ready;
    this.#context.processChanges(
      changes,
      () => (this.#ivmSources.main.hash = hash),
    );
  }
}
