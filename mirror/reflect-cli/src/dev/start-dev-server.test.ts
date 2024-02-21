import {afterEach, expect, jest, test} from '@jest/globals';
import getPort from 'get-port';
import {sleep} from 'shared/src/sleep.js';
import {MiniflareWrapper, fakeCrashForTesting} from './miniflare-wrapper.js';
import {getLogger} from '../logger.js';

afterEach(() => {
  jest.restoreAllMocks();
});

function getCode(i: number) {
  const encoder = new TextEncoder();
  return encoder.encode(`
  export default {
    fetch() {
      return new Response('${i}', {
        headers: {'content-type': 'application/json'},
      });
    }
  };
`);
}

test('restart', async () => {
  let i = 0;
  const port = await getPort();
  const mf = new MiniflareWrapper({
    port,
    modules: [
      {
        type: 'ESModule',
        path: 'dummy.js',
        get contents() {
          return getCode(i++);
        },
      },
    ],
  });
  const url = await mf.ready;

  const resp = await fetch(url);
  expect(await resp.json()).toBe(0);

  await mf.restart();

  {
    const resp = await fetch(url);
    expect(await resp.json()).toBe(1);
  }

  await mf.dispose();
});

test('induced fake crash', async () => {
  let i = 0;

  const log: unknown[] = [];
  jest
    .spyOn(getLogger(), 'error')
    .mockImplementation((...args) => log.push(args));

  const port = await getPort();
  const mf = new MiniflareWrapper({
    port,
    modules: [
      {
        type: 'ESModule',
        path: 'dummy.js',
        get contents() {
          return getCode(i++);
        },
      },
    ],
  });
  const url = await mf.ready;

  const resp = await fetch(url);
  expect(await resp.json()).toBe(0);

  fakeCrashForTesting();

  // Wait for stderr to propagate.
  await sleep(10);

  {
    await mf.ready;
    const resp = await fetch(url);
    expect(await resp.json()).toBe(1);
  }

  expect(log).toMatchInlineSnapshot(`
  [
    [
      "[31mSegmentation fault[39m",
    ],
    [
      "[31mDetected server crash...[39m",
    ],
  ]
  `);

  await mf.dispose();
});
