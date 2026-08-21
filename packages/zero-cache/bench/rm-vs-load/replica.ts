import {rmSync} from 'node:fs';
import type {LogContext} from '@rocicorp/logger';
import type {Database} from '../../../zqlite/src/db.ts';
import {StatementRunner} from '../../src/db/statements.ts';
import {ChangeProcessor} from '../../src/services/replicator/change-processor.ts';
import {initReplicationState} from '../../src/services/replicator/schema/replication-state.ts';
import {makeSchemaChanges} from './fixtures.ts';

export function initializeReplica(
  lc: LogContext,
  db: Database,
  replicaVersion: string,
): ChangeProcessor {
  initReplicationState(db, ['zero-cache-rm-vs-load'], replicaVersion);
  const processor = new ChangeProcessor(
    new StatementRunner(db),
    'serving',
    (_, err) => {
      throw err;
    },
  );
  processor.processMessage(lc, [
    'begin',
    {tag: 'begin'},
    {commitWatermark: '000000000001'},
  ]);
  for (const change of makeSchemaChanges()) {
    processor.processMessage(lc, ['data', change]);
  }
  processor.processMessage(lc, [
    'commit',
    {tag: 'commit'},
    {watermark: '000000000001'},
  ]);
  return processor;
}

export function cleanupSQLite(path: string) {
  rmSync(path, {force: true});
  rmSync(`${path}-shm`, {force: true});
  rmSync(`${path}-wal`, {force: true});
  rmSync(`${path}-wal2`, {force: true});
}
