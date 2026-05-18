import * as v from '../../../../shared/src/valita.ts';
import {changeStreamDataSchema} from '../change-source/protocol/current/downstream.ts';

export const CHANGE_STREAMER_V6_PROTOCOL_VERSION = 6;
export const CHANGE_STREAMER_V7_PROTOCOL_VERSION = 7;

const changeBatchPayloadSchema = v.object({
  tag: v.literal('change-batch'),
  changes: v.array(changeStreamDataSchema),
});

export const changeBatchMessageSchema = v.tuple([
  v.literal('change-batch'),
  changeBatchPayloadSchema,
]);

export type ChangeBatchMessage = v.Infer<typeof changeBatchMessageSchema>;

export function stringifyChangeBatch(changes: readonly string[]): string {
  if (changes.length === 0) {
    throw new Error('Cannot encode an empty change-batch frame');
  }
  // #6001: https://github.com/rocicorp/mono/pull/6001
  // The v7 RM -> VS protocol batches ordered replication changes into one
  // stream message so row-heavy transactions pay one parse/validation/ACK unit
  // per batch instead of one per row, while preserving existing
  // ChangeStreamData semantics.
  return `["change-batch",{"tag":"change-batch","changes":[${changes.join(
    ',',
  )}]}]`;
}
