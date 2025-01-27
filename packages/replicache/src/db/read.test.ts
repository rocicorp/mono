import {LogContext} from '@rocicorp/logger';
import {describe, expect, test} from 'vitest';
import type {Enum} from '../../../shared/src/enum.ts';
import {mustGetHeadHash} from '../dag/store.ts';
import {TestStore} from '../dag/test-store.ts';
import * as FormatVersion from '../format-version-enum.ts';
import {DEFAULT_HEAD_NAME} from './commit.ts';
import {readFromDefaultHead} from './read.ts';
import {initDB} from './test-helpers.ts';
import {newWriteLocal} from './write.ts';

type FormatVersion = Enum<typeof FormatVersion>;

describe('basics', () => {
  const t = async (replicacheFormatVersion: FormatVersion) => {
    const clientID = 'client-id';
    const dagStore = new TestStore();
    const lc = new LogContext();
    await initDB(
      await dagStore.write(),
      DEFAULT_HEAD_NAME,
      clientID,
      {},
      replicacheFormatVersion,
    );
    const dagWrite = await dagStore.write();
    const w = await newWriteLocal(
      await mustGetHeadHash(DEFAULT_HEAD_NAME, dagWrite),
      'mutator_name',
      JSON.stringify([]),
      null,
      dagWrite,
      42,
      clientID,
      replicacheFormatVersion,
    );
    await w.put(lc, 'foo', 'bar');
    await w.commit(DEFAULT_HEAD_NAME);

    const dagRead = await dagStore.read();
    const dbRead = await readFromDefaultHead(dagRead, replicacheFormatVersion);
    const val = await dbRead.get('foo');
    expect(val).to.deep.equal('bar');
  };
  test('dd31', () => t(FormatVersion.Latest));
});
