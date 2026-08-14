import {describe, expect, test} from 'vitest';
import {POKE_CHUNK_MESSAGE_TYPE} from '../../../zero-protocol/src/poke.ts';
import {PokeChunkEncoder} from './poke-chunk.ts';

describe('PokeChunkEncoder', () => {
  test('streams a JSON array in bounded chunks', async () => {
    const encoder = new PokeChunkEncoder(16);
    const chunks: Uint8Array[] = [];
    const emit = (chunk: Uint8Array) => {
      // The encoder deliberately reuses its output buffer.
      chunks.push(chunk.slice());
      return Promise.resolve();
    };

    await encoder.addPatch(JSON.stringify({a: '0123456789'}), emit);
    await encoder.addPatch(JSON.stringify({b: 2}), emit);
    await encoder.finish(emit);

    expect(chunks.every(chunk => chunk.byteLength <= 16)).toBe(true);
    expect(JSON.parse(joinAndDecode(chunks))).toEqual([
      {a: '0123456789'},
      {b: 2},
    ]);
  });

  test('splits a single oversized patch without corrupting UTF-8', async () => {
    const encoder = new PokeChunkEncoder(7);
    const chunks: Uint8Array[] = [];
    const emit = (chunk: Uint8Array) => {
      chunks.push(chunk.slice());
      return Promise.resolve();
    };
    const patch = {value: '🙂漢'.repeat(100)};

    await encoder.addPatch(JSON.stringify(patch), emit);
    await encoder.finish(emit);

    expect(chunks.length).toBeGreaterThan(1);
    expect(chunks.every(chunk => chunk.byteLength <= 7)).toBe(true);
    expect(JSON.parse(joinAndDecode(chunks))).toEqual([patch]);
  });

  test('bounds every frame for a ten-megabyte row', async () => {
    const encoder = new PokeChunkEncoder();
    const chunkLengths: number[] = [];
    const patch = JSON.stringify({value: 'x'.repeat(10 * 1024 * 1024)});

    await encoder.addPatch(patch, chunk => {
      chunkLengths.push(chunk.byteLength);
      return Promise.resolve();
    });
    await encoder.finish(chunk => {
      chunkLengths.push(chunk.byteLength);
      return Promise.resolve();
    });

    expect(chunkLengths).toHaveLength(11);
    expect(Math.max(...chunkLengths)).toBe(1024 * 1024);
    expect(chunkLengths.reduce((sum, length) => sum + length, 0)).toBe(
      patch.length + 2 + chunkLengths.length,
    );
  });

  test('encodes an empty poke as an empty array', async () => {
    const encoder = new PokeChunkEncoder();
    const chunks: Uint8Array[] = [];

    await encoder.finish(chunk => {
      chunks.push(chunk.slice());
      return Promise.resolve();
    });

    expect(joinAndDecode(chunks)).toBe('[]');
  });
});

function joinAndDecode(chunks: Uint8Array[]): string {
  const decoder = new TextDecoder('utf-8', {fatal: true});
  const decoded: string[] = [];
  for (const chunk of chunks) {
    expect(chunk[0]).toBe(POKE_CHUNK_MESSAGE_TYPE);
    decoded.push(decoder.decode(chunk.subarray(1), {stream: true}));
  }
  decoded.push(decoder.decode());
  return decoded.join('');
}
