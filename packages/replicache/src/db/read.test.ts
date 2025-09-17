/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members, @typescript-eslint/prefer-promise-reject-errors */
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
