import {describe, expect, test} from 'vitest';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import * as v from '../../../../shared/src/valita.ts';
import {ReplicationMessages} from '../replicator/test-utils.ts';
import {
  changeBatchMessageSchema,
  stringifyChangeBatch,
} from './change-streamer-protocol.ts';

describe('change-streamer/protocol', () => {
  test('encodes v7 change-batches without losing unsafe integers', () => {
    const messages = new ReplicationMessages({issues: 'id'});
    const insert = messages.insert('issues', {
      id: 'foo',
      big: BigInt(Number.MAX_SAFE_INTEGER) + 1n,
    });

    const encoded = stringifyChangeBatch([
      BigIntJSON.stringify(['data', insert]),
    ]);
    const parsed = v.parse(
      BigIntJSON.parse(encoded),
      changeBatchMessageSchema,
      'passthrough',
    );

    expect(parsed).toEqual([
      'change-batch',
      {tag: 'change-batch', changes: [['data', insert]]},
    ]);
  });

  test('rejects empty v7 change-batches', () => {
    expect(() => stringifyChangeBatch([])).toThrow(
      'Cannot encode an empty change-batch frame',
    );
  });
});
