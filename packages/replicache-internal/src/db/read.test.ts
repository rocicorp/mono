import {LogContext} from '@rocicorp/logger';
import {expect} from '@esm-bundle/chai';
import * as dag from '../dag/mod';
import {DEFAULT_HEAD_NAME} from './commit';
import {fromWhence, whenceHead} from './read';
import {initDB, Write} from './write';

test('basics', async () => {
  const ds = new dag.TestStore();
  const lc = new LogContext();
  await initDB(await ds.write(), DEFAULT_HEAD_NAME);
  const w = await Write.newLocal(
    whenceHead(DEFAULT_HEAD_NAME),
    'mutator_name',
    JSON.stringify([]),
    null,
    await ds.write(),
    42,
  );
  await w.put(lc, 'foo', 'bar');
  await w.commit(DEFAULT_HEAD_NAME);

  const dr = await ds.read();
  const r = await fromWhence(whenceHead(DEFAULT_HEAD_NAME), dr);
  const val = await r.get('foo');
  expect(val).to.deep.equal('bar');
});
