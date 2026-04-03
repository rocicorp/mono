/**
 * In-memory ZQL IVM benchmarks for hot-path deopt analysis.
 * Does NOT require postgres or SQLite - uses MemorySource only.
 *
 * Run:
 *   npm --workspace=zql-benchmarks run bench -- memory-ivm-deopt
 *
 * Run with deopt tracing (output goes to stderr inline):
 *   npm --workspace=zql-benchmarks run bench:deopt -- memory-ivm-deopt
 */

import {bench, describe} from '../../shared/src/bench.ts';
import type {Row} from '../../zero-protocol/src/data.ts';
import type {BuilderDelegate} from '../../zql/src/builder/builder.ts';
import {Catch} from '../../zql/src/ivm/catch.ts';
import {compareValues} from '../../zql/src/ivm/data.ts';
import {buildFilterPipeline} from '../../zql/src/ivm/filter-operators.ts';
import {Filter} from '../../zql/src/ivm/filter.ts';
import {Join} from '../../zql/src/ivm/join.ts';
import {MemorySource} from '../../zql/src/ivm/memory-source.ts';

const noopDelegate = {addEdge() {}} as unknown as BuilderDelegate;

// ---- Schema definitions ----

const issueColumns = {
  id: {type: 'string'} as const,
  title: {type: 'string'} as const,
  closed: {type: 'boolean'} as const,
  ownerId: {type: 'string', optional: true} as const,
  createdAt: {type: 'number'} as const,
};

const userColumns = {
  id: {type: 'string'} as const,
  name: {type: 'string'} as const,
};

// ---- Row generators ----

function makeIssueRow(i: number): Row {
  return {
    id: `issue-${i}`,
    title: `Issue ${i}`,
    closed: i % 3 === 0,
    ownerId: i % 5 === 0 ? null : `user-${i % 20}`,
    createdAt: 1_000_000 + i,
  };
}

function makeUserRow(i: number): Row {
  return {id: `user-${i}`, name: `User ${i}`};
}

const ISSUE_COUNT = 1000;
const USER_COUNT = 20;

// ---- compareValues (hot path, deopt risk from polymorphism) ----

describe('compareValues', () => {
  const strs = ['apple', 'banana', 'cherry', 'date', 'elderberry'];
  const nums = [1, 2, 3, 4, 5];

  bench('compareValues(string, string)', () => {
    for (let i = 0; i < strs.length; i++) {
      for (let j = 0; j < strs.length; j++) {
        compareValues(strs[i], strs[j]);
      }
    }
  });

  bench('compareValues(number, number)', () => {
    for (let i = 0; i < nums.length; i++) {
      for (let j = 0; j < nums.length; j++) {
        compareValues(nums[i], nums[j]);
      }
    }
  });

  // Mixed types through the same call site — classic deopt trigger.
  bench('compareValues(mixed: string/number/bool/null)', () => {
    compareValues('a', 'b');
    compareValues(1, 2);
    compareValues(true, false);
    compareValues(null, null);
  });
});

// ---- MemorySource fetch ----

describe('MemorySource fetch', () => {
  bench(`scan ${ISSUE_COUNT} rows`, function* () {
    const src = new MemorySource('issue', issueColumns, ['id']);
    for (let i = 0; i < ISSUE_COUNT; i++) {
      for (const _ of src.push({type: 'add', row: makeIssueRow(i)})) {
        /* consume */
      }
    }
    const conn = src.connect([
      ['createdAt', 'asc'],
      ['id', 'asc'],
    ]);
    const out = new Catch(conn);

    yield () => {
      out.fetch();
    };
  });
});

// ---- MemorySource push ----

describe('MemorySource push', () => {
  bench(`add/remove over ${ISSUE_COUNT} rows`, function* () {
    const src = new MemorySource('issue', issueColumns, ['id']);
    for (let i = 0; i < ISSUE_COUNT; i++) {
      for (const _ of src.push({type: 'add', row: makeIssueRow(i)})) {
        /* consume */
      }
    }
    const conn = src.connect([
      ['createdAt', 'asc'],
      ['id', 'asc'],
    ]);
    new Catch(conn);

    let idx = ISSUE_COUNT;
    yield () => {
      const row = makeIssueRow(idx++);
      for (const _ of src.push({type: 'add', row})) {
        /* consume */
      }
      for (const _ of src.push({type: 'remove', row})) {
        /* consume */
      }
    };
  });

  bench(`edit over ${ISSUE_COUNT} rows`, function* () {
    const src = new MemorySource('issue', issueColumns, ['id']);
    const rows: Row[] = [];
    for (let i = 0; i < ISSUE_COUNT; i++) {
      const row = makeIssueRow(i);
      rows.push(row);
      for (const _ of src.push({type: 'add', row})) {
        /* consume */
      }
    }
    const conn = src.connect([
      ['createdAt', 'asc'],
      ['id', 'asc'],
    ]);
    new Catch(conn);

    let idx = 0;
    yield () => {
      const oldRow = rows[idx % rows.length];
      idx++;
      const newRow = {...oldRow, title: `Updated ${idx}`};
      for (const _ of src.push({type: 'edit', oldRow, row: newRow})) {
        /* consume */
      }
      for (const _ of src.push({type: 'edit', oldRow: newRow, row: oldRow})) {
        /* consume */
      }
    };
  });
});

// ---- Filter ----

describe('Filter', () => {
  bench(`fetch open issues (${ISSUE_COUNT} total)`, function* () {
    const src = new MemorySource('issue', issueColumns, ['id']);
    for (let i = 0; i < ISSUE_COUNT; i++) {
      for (const _ of src.push({type: 'add', row: makeIssueRow(i)})) {
        /* consume */
      }
    }
    const conn = src.connect([
      ['createdAt', 'asc'],
      ['id', 'asc'],
    ]);
    const filtered = buildFilterPipeline(
      conn,
      noopDelegate,
      input => new Filter(input, row => row['closed'] === false),
    );
    const out = new Catch(filtered);

    yield () => {
      out.fetch();
    };
  });

  bench('push add open issue (passes filter)', function* () {
    const src = new MemorySource('issue', issueColumns, ['id']);
    for (let i = 0; i < ISSUE_COUNT; i++) {
      for (const _ of src.push({type: 'add', row: makeIssueRow(i)})) {
        /* consume */
      }
    }
    const conn = src.connect([
      ['createdAt', 'asc'],
      ['id', 'asc'],
    ]);
    const filtered = buildFilterPipeline(
      conn,
      noopDelegate,
      input => new Filter(input, row => row['closed'] === false),
    );
    new Catch(filtered);

    let idx = ISSUE_COUNT;
    yield () => {
      const row = makeIssueRow(idx++);
      (row as Record<string, unknown>)['closed'] = false;
      for (const _ of src.push({type: 'add', row})) {
        /* consume */
      }
      for (const _ of src.push({type: 'remove', row})) {
        /* consume */
      }
    };
  });

  bench('push add closed issue (filtered out)', function* () {
    const src = new MemorySource('issue', issueColumns, ['id']);
    for (let i = 0; i < ISSUE_COUNT; i++) {
      for (const _ of src.push({type: 'add', row: makeIssueRow(i)})) {
        /* consume */
      }
    }
    const conn = src.connect([
      ['createdAt', 'asc'],
      ['id', 'asc'],
    ]);
    const filtered = buildFilterPipeline(
      conn,
      noopDelegate,
      input => new Filter(input, row => row['closed'] === false),
    );
    new Catch(filtered);

    let idx = ISSUE_COUNT;
    yield () => {
      const row = makeIssueRow(idx++);
      (row as Record<string, unknown>)['closed'] = true;
      for (const _ of src.push({type: 'add', row})) {
        /* consume */
      }
      for (const _ of src.push({type: 'remove', row})) {
        /* consume */
      }
    };
  });
});

// ---- Join ----

describe('Join', () => {
  bench(`fetch ${ISSUE_COUNT} issues → ${USER_COUNT} users`, function* () {
    const issueSrc = new MemorySource('issue', issueColumns, ['id']);
    const userSrc = new MemorySource('user', userColumns, ['id']);

    for (let i = 0; i < USER_COUNT; i++) {
      for (const _ of userSrc.push({type: 'add', row: makeUserRow(i)})) {
        /* consume */
      }
    }
    for (let i = 0; i < ISSUE_COUNT; i++) {
      for (const _ of issueSrc.push({type: 'add', row: makeIssueRow(i)})) {
        /* consume */
      }
    }

    const issueConn = issueSrc.connect([
      ['createdAt', 'asc'],
      ['id', 'asc'],
    ]);
    const userConn = userSrc.connect([['id', 'asc']]);
    const join = new Join({
      parent: issueConn,
      child: userConn,
      parentKey: ['ownerId'],
      childKey: ['id'],
      relationshipName: 'owner',
      hidden: false,
      system: 'client',
    });
    const out = new Catch(join);

    yield () => {
      out.fetch();
    };
  });

  bench('push add/remove issue with owner', function* () {
    const issueSrc = new MemorySource('issue', issueColumns, ['id']);
    const userSrc = new MemorySource('user', userColumns, ['id']);

    for (let i = 0; i < USER_COUNT; i++) {
      for (const _ of userSrc.push({type: 'add', row: makeUserRow(i)})) {
        /* consume */
      }
    }
    for (let i = 0; i < ISSUE_COUNT; i++) {
      for (const _ of issueSrc.push({type: 'add', row: makeIssueRow(i)})) {
        /* consume */
      }
    }

    const issueConn = issueSrc.connect([
      ['createdAt', 'asc'],
      ['id', 'asc'],
    ]);
    const userConn = userSrc.connect([['id', 'asc']]);
    const join = new Join({
      parent: issueConn,
      child: userConn,
      parentKey: ['ownerId'],
      childKey: ['id'],
      relationshipName: 'owner',
      hidden: false,
      system: 'client',
    });
    new Catch(join);
    join.fetch({});

    let idx = ISSUE_COUNT;
    yield () => {
      const row = makeIssueRow(idx++);
      for (const _ of issueSrc.push({type: 'add', row})) {
        /* consume */
      }
      for (const _ of issueSrc.push({type: 'remove', row})) {
        /* consume */
      }
    };
  });

  bench('push edit issue title (non-key field)', function* () {
    const issueSrc = new MemorySource('issue', issueColumns, ['id']);
    const userSrc = new MemorySource('user', userColumns, ['id']);

    for (let i = 0; i < USER_COUNT; i++) {
      for (const _ of userSrc.push({type: 'add', row: makeUserRow(i)})) {
        /* consume */
      }
    }
    const issueRows: Row[] = [];
    for (let i = 0; i < ISSUE_COUNT; i++) {
      const row = makeIssueRow(i);
      issueRows.push(row);
      for (const _ of issueSrc.push({type: 'add', row})) {
        /* consume */
      }
    }

    const issueConn = issueSrc.connect([
      ['createdAt', 'asc'],
      ['id', 'asc'],
    ]);
    const userConn = userSrc.connect([['id', 'asc']]);
    const join = new Join({
      parent: issueConn,
      child: userConn,
      parentKey: ['ownerId'],
      childKey: ['id'],
      relationshipName: 'owner',
      hidden: false,
      system: 'client',
    });
    new Catch(join);
    join.fetch({});

    let idx = 0;
    yield () => {
      const oldRow = issueRows[idx % issueRows.length];
      idx++;
      const newRow = {...oldRow, title: `Updated ${idx}`};
      for (const _ of issueSrc.push({type: 'edit', oldRow, row: newRow})) {
        /* consume */
      }
      for (const _ of issueSrc.push({
        type: 'edit',
        oldRow: newRow,
        row: oldRow,
      })) {
        /* consume */
      }
    };
  });
});
