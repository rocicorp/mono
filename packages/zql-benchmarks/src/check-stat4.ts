/* oxlint-disable no-console */
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {Database, type Statement} from '../../zqlite/src/db.ts';

// SQLITE_SCANSTAT_EST = 2 (estimated rows). We'll query a few literals to see
// whether SQLite consults stat4 for them.
const SQLITE_SCANSTAT_EST = 2;
const SQLITE_SCANSTAT_EXPLAIN = 4;

const db = new Database(
  createSilentLogContext(),
  '/Users/mlaw/workspace/tera.db',
);

function estimate(sql: string) {
  const stmt: Statement = db.prepare(sql);
  for (let idx = 0; ; idx++) {
    const explain = stmt.scanStatus(idx, SQLITE_SCANSTAT_EXPLAIN, 1);
    if (explain === undefined) break;
    const est = stmt.scanStatus(idx, SQLITE_SCANSTAT_EST, 1);
    console.log(`  ${explain}  (est=${est})`);
  }
}

const literals = [
  'lbl_proj_000_15', // api-gateway — observed actual: 416,626
  'lbl_proj_000_06', // enhancement — actually queried in variant 3
  'lbl_proj_000_01', // a sampled label (in stat4)
  'lbl_proj_044_21', // a sampled label with much smaller neq (~31,485)
  'lbl_proj_999_99', // nonexistent / outside sample range
];

for (const lit of literals) {
  console.log(`\nlabelID = '${lit}'`);
  estimate(`SELECT * FROM "issueLabel" WHERE "labelID" = '${lit}'`);
}

console.log(
  '\n-- planner-style query (the actual variant-1 issueLabel driver) --',
);
estimate(
  `SELECT "labelID","issueID","projectID","_0_version" FROM "issueLabel" ` +
    `WHERE "labelID" = 'lbl_proj_000_15' ORDER BY "labelID" asc, "issueID" asc`,
);

db.close();
