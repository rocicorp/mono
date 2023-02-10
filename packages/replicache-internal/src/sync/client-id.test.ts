import {expect} from '@esm-bundle/chai';
import {MemStore} from '../kv/mod';
import {init} from './client-id';

test('init client ID', async () => {
  const ms = new MemStore();
  const cid1 = await init(ms);
  const cid2 = await init(ms);
  expect(cid1).to.equal(cid2);
  const ms2 = new MemStore();
  const cid3 = await init(ms2);
  expect(cid1).to.not.equal(cid3);
});
