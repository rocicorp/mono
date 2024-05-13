import {expect, test} from 'vitest';
// @ts-expect-error TS does not like this import
import wasmModule from './xxhash.wasm';

test('xxhash sanity checking', async () => {
  const wasmInstance = await WebAssembly.instantiate(wasmModule);
  expect(wasmInstance.exports).toMatchInlineSnapshot(`
    {
      "digest32": [Function],
      "digest64": [Function],
      "init32": [Function],
      "init64": [Function],
      "mem": Memory {},
      "update32": [Function],
      "update64": [Function],
      "xxh32": [Function],
      "xxh64": [Function],
    }
  `);
});
