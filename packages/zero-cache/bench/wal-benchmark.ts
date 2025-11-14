import {statSync} from 'fs';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {randInt} from '../../shared/src/rand.ts';
import type {Statement} from '../../zqlite/src/db.ts';
import {Database} from '../../zqlite/src/db.ts';
import {bench} from './benchmark.ts';

const lc = createSilentLogContext();

type Options = {
  dbFile: string;
  mode: 'WAL' | 'WAL2';
  runs: number;
  modify: number;
};

class Reader {
  readonly #db: Database;
  readonly #begin: Statement;
  readonly #rollback: Statement;

  constructor(dbFile: string) {
    this.#db = new Database(lc, dbFile, {readonly: true});
    this.#begin = this.#db.prepare('begin immediate');
    this.#rollback = this.#db.prepare('rollback');
  }

  relock(): this {
    if (this.#db.inTransaction) {
      this.#rollback.run();
    }
    this.#begin.run();
    return this;
  }

  close() {
    this.#db.close();
  }
}

export function walBenchmark(opts: Options) {
  const {dbFile, mode, runs, modify} = opts;
  const db = new Database(lc, dbFile);
  // Start from scratch.
  db.pragma(`journal_mode = delete`);
  db.pragma(`journal_mode = ${mode}`);
  db.pragma('wal_checkpoint(TRUNCATE)');

  // Lock the database to prevent WAL checkpointing.
  const reader1 = new Reader(dbFile).relock();
  const reader2 = new Reader(dbFile).relock();

  const ids = db
    .prepare('select id from issue')
    .all<{id: string}>()
    .map(row => row.id);
  let {maxModified} = db
    .prepare('select max(modified) as maxModified from issue')
    .get<{maxModified: number}>();
  const perturb = db.prepare(`UPDATE issue SET modified=? WHERE id = ?`);

  // oxlint-disable-next-line no-console
  console.log(`warmup`);
  bench(opts);

  // oxlint-disable-next-line no-console
  console.log(`\nmodifying ${opts.modify} rows per iteration`);
  for (let i = 0; i < runs; i++) {
    // oxlint-disable-next-line no-console
    console.log(
      `\n${mode}\tsize`,
      (
        (statSync(`${dbFile}-wal`).size +
          (mode === 'WAL2' ? statSync(`${dbFile}-wal2`).size : 0)) /
        1024 /
        1024
      ).toPrecision(4),
      'MB',
    );
    bench(opts);

    const start = performance.now();
    // Perturb random entries in the db.
    randomEntries(ids, modify).forEach(id => perturb.run(++maxModified, id));
    const end = performance.now();

    // oxlint-disable-next-line no-console
    console.log(`modify\ttook ${end - start}ms`);

    // "Advance" the readers but maintain a hold on the lock.
    reader1.relock();
    reader2.relock();
  }

  reader1.close();
  reader2.close();
  db.close();
}

function randomEntries(source: string[], count: number): string[] {
  const result: string[] = [];
  for (let i = 0; i < count; i++) {
    result.push(source[randInt(0, source.length - 1)]);
  }
  return result;
}
