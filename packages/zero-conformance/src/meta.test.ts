import {test} from 'vitest';
import {
  MIN_SERVER_SUPPORTED_SYNC_PROTOCOL,
  PROTOCOL_VERSION,
} from '../../zero-protocol/src/protocol-version.ts';
import {emitVectors} from './vectors.ts';

test('meta', () => {
  emitVectors(
    'meta',
    'meta',
    'Protocol versions of the JS implementation the vectors were generated from.',
    [
      {
        protocolVersion: PROTOCOL_VERSION,
        minServerSupportedSyncProtocol: MIN_SERVER_SUPPORTED_SYNC_PROTOCOL,
      },
    ],
  );
});
