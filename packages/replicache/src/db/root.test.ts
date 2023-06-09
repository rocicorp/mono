import {expect} from 'chai';
import * as dag from '../dag/mod.js';
import {fakeHash, Hash} from '../hash.js';
import {withWrite} from '../with-transactions.js';
import {DEFAULT_HEAD_NAME} from './commit.js';
import {getRoot} from './root.js';

test('getRoot', async () => {
  const t = async (headHash: Hash | undefined, expected: Hash | Error) => {
    const ds = new dag.TestStore();
    if (headHash !== undefined) {
      await withWrite(ds, async dw => {
        await dw.setHead(DEFAULT_HEAD_NAME, headHash);
        await dw.commit();
      });
    }
    if (expected instanceof Error) {
      let err;
      try {
        await getRoot(ds, DEFAULT_HEAD_NAME);
      } catch (e) {
        err = e;
      }
      expect(err).to.be.an.instanceof(Error);
      expect((err as Error).message).to.equal(expected.message);
    } else {
      const actual = await getRoot(ds, DEFAULT_HEAD_NAME);
      expect(actual).to.equal(expected);
    }
  };

  await t(undefined, new Error('No head found for main'));
  const foo = fakeHash('f00');
  await t(foo, foo);
});
