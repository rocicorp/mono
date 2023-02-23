import {LogContext} from '@rocicorp/logger';
import {expect} from '@esm-bundle/chai';
import * as dag from '../dag/mod.js';
import {DEFAULT_HEAD_NAME} from './commit.js';
import {fromWhence, whenceHead} from './read.js';
import {newWriteLocal} from './write.js';
import {initDB} from './test-helpers.js';

suite('basics', () => {
  const t = async (dd31: boolean) => {
    const clientID = 'client-id';
    const ds = new dag.TestStore();
    const lc = new LogContext();
    await initDB(await ds.write(), DEFAULT_HEAD_NAME, clientID, {}, dd31);
    const w = await newWriteLocal(
      whenceHead(DEFAULT_HEAD_NAME),
      'mutator_name',
      JSON.stringify([]),
      null,
      await ds.write(),
      42,
      clientID,
      dd31,
    );
    await w.put(lc, 'foo', 'bar');
    await w.commit(DEFAULT_HEAD_NAME);

    const dr = await ds.read();
    const r = await fromWhence(whenceHead(DEFAULT_HEAD_NAME), dr);
    const val = await r.get('foo');
    expect(val).to.deep.equal('bar');
  };
  test('dd31', () => t(true));
  test('sdd', () => t(false));
});
