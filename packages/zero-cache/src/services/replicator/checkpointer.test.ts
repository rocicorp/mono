import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {Database} from 'zqlite/src/db.js';
import {DbFile} from '../../test/lite.js';
import {WALCheckpointer} from './checkpointer.js';
import {Notifier} from './notifier.js';

describe('replicator/checkpointer', () => {
  let dbFile: DbFile;
  let db: Database;
  let viewSyncer: Database;
  let notifier: Notifier;
  let checkpointer: WALCheckpointer;

  const lc = createSilentLogContext();

  beforeEach(() => {
    dbFile = new DbFile('checkpointer');
    db = dbFile.connect(lc);
    db.pragma('journal_mode = WAL');
    db.pragma('synchronous = NORMAL');
    db.exec(`
      CREATE TABLE foo(id INTEGER PRIMARY KEY);
      INSERT INTO foo(id) VALUES(1);
      INSERT INTO foo(id) VALUES(2);
      INSERT INTO foo(id) VALUES(3);
    `);

    viewSyncer = dbFile.connect(lc);
    notifier = new Notifier();
    checkpointer = new WALCheckpointer(lc, dbFile.path, {threshold: 5});
  });

  afterEach(async () => {
    checkpointer.stop();
    await dbFile.unlink();
  });

  test('checkpointing', {timeout: 100}, async () => {
    const lock = viewSyncer.prepare('begin immediate');
    const unlock = viewSyncer.prepare('rollback');
    lock.run();

    const subscription = notifier.subscribe();
    void (async function () {
      for await (const {state} of subscription) {
        if (state === 'maintenance') {
          unlock.run();
        } else if (!viewSyncer.inTransaction) {
          lock.run();
        }
      }
    })();

    // Checkpoints should fail while the viewSyncer has a lock.
    expect(viewSyncer.inTransaction);

    db.pragma('busy_timeout = 1');
    expect(db.pragma('wal_checkpoint(TRUNCATE)')).toMatchObject([
      {busy: 1, log: 5},
    ]);

    // The checkpointer should signal a 'maintenance' mode and execute a
    // checkpoint with the view syncer unlocked.
    await checkpointer.maybeCheckpoint(10, notifier);

    // Upon completion, the view syncer should be locked again.
    expect(viewSyncer.inTransaction);

    // But the checkpoint should have succeeded.
    expect(db.pragma('wal_checkpoint(TRUNCATE)')).toMatchObject([
      {busy: 1, log: 0},
    ]);
  });
});
