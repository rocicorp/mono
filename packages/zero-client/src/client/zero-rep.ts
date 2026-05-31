import type {
  InternalDiff,
  InternalDiffOperation,
} from '../../../replicache/src/btree/node.ts';
import type {LazyStore} from '../../../replicache/src/dag/lazy-store.ts';
import {readFromHash} from '../../../replicache/src/db/read.ts';
import * as FormatVersion from '../../../replicache/src/format-version-enum.ts';
import type {Hash} from '../../../replicache/src/hash.ts';
import type {
  EphemeralID,
  MutationTrackingData,
  ZeroOption,
  ZeroReadOptions,
} from '../../../replicache/src/replicache-options.ts';
import {withRead} from '../../../replicache/src/with-transactions.ts';
import {must} from '../../../shared/src/must.ts';
import type {ZeroContext} from './context.ts';
import type {IVMSourceBranch} from './ivm-branch.ts';
import {ENTITIES_KEY_PREFIX} from './keys.ts';
import type {MutationTracker} from './mutation-tracker.ts';

// Replicache doesn't support undefined auth tokens
const REPLICACHE_NO_AUTH_TOKEN = '';

export const toReplicacheAuthToken = (
  auth: string | undefined | null,
): string => auth ?? REPLICACHE_NO_AUTH_TOKEN;

export const fromReplicacheAuthToken = (auth: string): string | undefined =>
  !auth ? undefined : auth;

type TxData = {
  ivmSources: IVMSourceBranch;
  token: string | undefined;
  context: unknown;
};

export class ZeroRep implements ZeroOption {
  readonly #context: ZeroContext;
  readonly #ivmMain: IVMSourceBranch;
  readonly #customMutatorsEnabled: boolean;
  readonly #mutationTracker: MutationTracker;
  #store: LazyStore | undefined;
  // matches replicache's auth token type
  #auth: string = REPLICACHE_NO_AUTH_TOKEN;

  constructor(
    context: ZeroContext,
    ivmMain: IVMSourceBranch,
    customMutatorsEnabled: boolean,
    mutationTracker: MutationTracker,
  ) {
    this.#context = context;
    this.#ivmMain = ivmMain;
    this.#customMutatorsEnabled = customMutatorsEnabled;
    this.#mutationTracker = mutationTracker;
  }

  set auth(auth: string) {
    this.#auth = auth;
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
    readOptions?: ZeroReadOptions,
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
        token: fromReplicacheAuthToken(this.#auth),
        context: this.#context,
      }));
  };

  advance = (expectedHash: Hash, newHash: Hash, diffs: InternalDiff): void => {
    this.#context.processChanges(expectedHash, newHash, diffs);
  };

  trackMutation(): MutationTrackingData {
    return this.#mutationTracker.trackMutation();
  }
  mutationIDAssigned(ephemeralID: EphemeralID, mutationID: number): void {
    this.#mutationTracker.mutationIDAssigned(ephemeralID, mutationID);
  }
  rejectMutation(ephemeralID: EphemeralID, ex: unknown): void {
    this.#mutationTracker.rejectMutation(ephemeralID, ex);
  }
}
