import {statSync} from 'node:fs';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {DbFile} from '../../../test/lite.ts';
import {initReplicationState} from '../../replicator/schema/replication-state.ts';
import {initializeStaticChangeSource} from './change-source.ts';

describe('change-source/static', () => {
  let dbFile: DbFile;

  beforeEach(() => {
    dbFile = new DbFile('static-change-source');
  });

  afterEach(() => {
    dbFile.delete();
  });

  test('reads watermark/replicaVersion from _zero meta tables', () => {
    const lc = createSilentLogContext();
    const seed = dbFile.connect(lc);
    initReplicationState(seed, ['pub1', 'pub2'], '01abcd');
    seed.close();

    const {subscriptionState, changeSource} = initializeStaticChangeSource(
      lc,
      dbFile.path,
    );
    expect(subscriptionState.watermark).toBe('01abcd');
    expect(subscriptionState.replicaVersion).toBe('01abcd');
    expect(subscriptionState.publications).toEqual(['pub1', 'pub2']);
    expect(changeSource.startLagReporter()).toBeNull();
  });

  test('startStream returns a stream that does not emit and cancels cleanly', async () => {
    const lc = createSilentLogContext();
    const seed = dbFile.connect(lc);
    initReplicationState(seed, ['pub'], '02deadbeef');
    seed.close();

    const {changeSource, subscriptionState} = initializeStaticChangeSource(
      lc,
      dbFile.path,
    );
    const stream = await changeSource.startStream(subscriptionState.watermark);

    let received = 0;
    const consume = (async () => {
      for await (const _ of stream.changes) {
        received++;
      }
    })();

    // Give the iterator a tick; nothing should be emitted.
    await new Promise(r => setTimeout(r, 25));
    expect(received).toBe(0);

    stream.changes.cancel();
    await consume;
    expect(received).toBe(0);

    await changeSource.stop();
  });

  test('does not modify the replica file', () => {
    const lc = createSilentLogContext();
    const seed = dbFile.connect(lc);
    initReplicationState(seed, ['pub'], '03cafebabe');
    seed.close();

    const before = statSync(dbFile.path).mtimeMs;

    initializeStaticChangeSource(lc, dbFile.path);

    const after = statSync(dbFile.path).mtimeMs;
    expect(after).toBe(before);
  });
});
