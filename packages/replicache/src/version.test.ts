import {expect} from 'chai';
import {version} from './version.js';

test('version', async () => {
  expect(version).is.string;
  const x = await fetch(new URL('../package.json', import.meta.url));
  expect(version).equal((await x.json()).version);
});
