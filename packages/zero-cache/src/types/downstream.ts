import type {Downstream} from '../../../zero-protocol/src/down.ts';

/**
 * A logical view-syncer message and, when already available, its serialized
 * wire representation. Keeping them together lets the connection reuse the
 * view-syncer's serialization instead of stringifying large poke parts again.
 */
export type ViewSyncerDownstream = {
  readonly message: Downstream;
  readonly serialized: string | undefined;
};
