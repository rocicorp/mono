import {expect, test} from 'vitest';
import {appendPath, decodePokeChunks, toWebSocketURL} from './sync-client.ts';

test('decodePokeChunks joins chunks split inside a code point', () => {
  const parts = [
    {pokeID: 'p1', rowsPatch: [{op: 'put', tableName: 't', value: {s: 'é✓'}}]},
  ];
  const bytes = new TextEncoder().encode(JSON.stringify(parts));
  const split = bytes.indexOf(0xc3) + 1;
  expect(
    decodePokeChunks([bytes.subarray(0, split), bytes.subarray(split)]),
  ).toEqual(parts);
});

test('toWebSocketURL and appendPath', () => {
  expect(toWebSocketURL('http://127.0.0.1:4848')).toBe('ws://127.0.0.1:4848');
  expect(toWebSocketURL('https://zero.example/base')).toBe(
    'wss://zero.example/base',
  );
  expect(appendPath('wss://zero.example/base/', '/sync/v52/connect')).toBe(
    'wss://zero.example/base/sync/v52/connect',
  );
});
