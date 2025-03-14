import type {
  InternalDiff,
  InternalDiffOperation,
} from '../../../replicache/src/btree/node.ts';
import {readFromHash} from '../../../replicache/src/db/read.ts';
import type {Hash} from '../../../replicache/src/hash.ts';
import {withRead} from '../../../replicache/src/with-transactions.ts';
import type {ZeroContext} from './context.ts';
import * as FormatVersion from '../../../replicache/src/format-version-enum.ts';
import type {IVMSourceBranch} from './ivm-branch.ts';
import {ENTITIES_KEY_PREFIX} from './keys.ts';
import {must} from '../../../shared/src/must.ts';
import type {LazyStore} from '../../../replicache/src/dag/lazy-store.ts';
import type {
  ZeroOption,
  ZeroReadOptions,
} from '../../../replicache/src/replicache-options.ts';

type TxData = {
  ivmSources: IVMSourceBranch;
  token: string | undefined;
};

export class ZeroRep implements ZeroOption {
  readonly #context: ZeroContext;
  readonly #ivmMain: IVMSourceBranch;
  readonly #customMutatorsEnabled: boolean;
  #store: LazyStore | undefined;
  #auth: string | undefined;

  constructor(
    context: ZeroContext,
    ivmMain: IVMSourceBranch,
    customMutatorsEnabled: boolean,
  ) {
    this.#context = context;
    this.#ivmMain = ivmMain;
    this.#customMutatorsEnabled = customMutatorsEnabled;
  }

  set auth(auth: string) {
    if (auth === '') {
      this.#auth = undefined;
    } else {
      this.#auth = auth;
    }
  }

  async init(hash: Hash, store: LazyStore) {
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
    this.#store = store;

    this.#context.processChanges(undefined, hash, diffs);
  }

  getTxData = (
    desiredHead: Hash,
    readOptions?: ZeroReadOptions | undefined,
  ): Promise<TxData> | undefined => {
    // getTxData requires some extensive testing for complete confidence
    // that it will not break. Do not enable `getTxData` unless the user
    // has opted into custom mutators.
    if (!this.#customMutatorsEnabled) {
      return;
    }

    return this.#ivmMain
      .forkToHead(must(this.#store), desiredHead, readOptions)
      .then(branch => ({
        ivmSources: branch,
        token: this.#auth,
      }));
  };

  advance = (expectedHash: Hash, newHash: Hash, diffs: InternalDiff): void => {
    this.#context.processChanges(expectedHash, newHash, diffs);
  };
}
