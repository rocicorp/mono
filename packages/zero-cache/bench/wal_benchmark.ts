import {statSync} from 'fs';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {randInt} from 'shared/src/rand.js';
import {Database} from 'zqlite/src/db.js';
import {bench} from './benchmark.js';

const DB_FILE = '/tmp/sync-replica.db';

const lc = createSilentLogContext();

type Options = {
  runs: number;
  modify: number;
};

export function wal_benchmark(opts: Options) {
  const db = new Database(lc, DB_FILE);
  // Start from scratch.
  db.pragma('wal_checkpoint(TRUNCATE)');

  // Lock the database to prevent WAL checkpointing.
  const lock = new Database(lc, DB_FILE, {readonly: true});
  lock.exec('begin immediate');

  const ids = db
    .prepare('select id from issue')
    .all<{id: string}>()
    .map(row => row.id);
  let {maxModified} = db
    .prepare('select max(modified) as maxModified from issue')
    .get<{maxModified: number}>();
  const perturb = db.prepare(`UPDATE issue SET modified=? WHERE id = ?`);

  console.log(`warmup`);
  bench();

  console.log(`\nmodifying ${opts.modify} rows per iteration`);
  for (let i = 0; i < opts.runs; i++) {
    console.log(
      '\nWAL     size',
      (statSync(`${DB_FILE}-wal`).size / 1024 / 1024).toPrecision(4),
      'MB',
    );
    bench();

    const start = performance.now();
    // Perturb random entries in the db.
    randomEntries(ids, opts.modify).forEach(id =>
      perturb.run(++maxModified, id),
    );
    const end = performance.now();

    console.log(`modify  took ${end - start}ms`);
  }
}

function randomEntries(source: string[], count: number): string[] {
  const result: string[] = [];
  for (let i = 0; i < count; i++) {
    result.push(source[randInt(0, source.length - 1)]);
  }
  return result;
}
