import {expect, test} from 'vitest';
import {version} from './version.ts';

test('version', async () => {
  expect(version).is.string;
  const x = await fetch(new URL('../package.json', import.meta.url));
  expect(version).toBe((await x.json()).version);
});
