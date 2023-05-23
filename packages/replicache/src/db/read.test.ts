import {expect} from '@esm-bundle/chai';
import {LogContext} from '@rocicorp/logger';
import * as dag from '../dag/mod.js';
import {
  REPLICACHE_FORMAT_VERSION,
  REPLICACHE_FORMAT_VERSION_SDD,
  ReplicacheFormatVersion,
} from '../format-version.js';
import {DEFAULT_HEAD_NAME} from './commit.js';
import {fromWhence, whenceHead} from './read.js';
import {initDB} from './test-helpers.js';
import {newWriteLocal} from './write.js';

suite('basics', () => {
  const t = async (replicacheFormatVersion: ReplicacheFormatVersion) => {
    const clientID = 'client-id';
    const ds = new dag.TestStore();
    const lc = new LogContext();
    await initDB(
      await ds.write(),
      DEFAULT_HEAD_NAME,
      clientID,
      {},
      replicacheFormatVersion,
    );
    const w = await newWriteLocal(
      whenceHead(DEFAULT_HEAD_NAME),
      'mutator_name',
      JSON.stringify([]),
      null,
      await ds.write(),
      42,
      clientID,
      replicacheFormatVersion,
    );
    await w.put(lc, 'foo', 'bar');
    await w.commit(DEFAULT_HEAD_NAME);

    const dr = await ds.read();
    const r = await fromWhence(whenceHead(DEFAULT_HEAD_NAME), dr);
    const val = await r.get('foo');
    expect(val).to.deep.equal('bar');
  };
  test('dd31', () => t(REPLICACHE_FORMAT_VERSION));
  test('sdd', () => t(REPLICACHE_FORMAT_VERSION_SDD));
});
