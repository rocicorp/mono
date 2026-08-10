import type {Downstream} from '../../../zero-protocol/src/down.ts';

export type ViewSyncerDownstream = {
  readonly message: Downstream;
  readonly serialized: string | undefined;
};
