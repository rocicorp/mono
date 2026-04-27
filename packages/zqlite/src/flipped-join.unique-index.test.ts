import {expect, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import type {CompoundKey, Ordering} from '../../zero-protocol/src/ast.ts';
import type {Row} from '../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../zero-protocol/src/primary-key.ts';
import type {SchemaValue} from '../../zero-schema/src/table-schema.ts';
import {Catch, type CaughtNode} from '../../zql/src/ivm/catch.ts';
import {FlippedJoin} from '../../zql/src/ivm/flipped-join.ts';
import type {FetchRequest} from '../../zql/src/ivm/operator.ts';
import {makeSourceChangeAdd} from '../../zql/src/ivm/source.ts';
import {consume} from '../../zql/src/ivm/stream.ts';
import {Database} from './db.ts';
import {TableSource} from './table-source.ts';

const lc = createSilentLogContext();

function makeSource(
  db: Database,
  name: string,
  columns: Record<string, SchemaValue>,
  primaryKey: PrimaryKey,
  rows: readonly Row[],
) {
  const source = new TableSource(
    lc,
    testLogConfig,
    db,
    name,
    columns,
    primaryKey,
  );
  for (const row of rows) {
    consume(source.push(makeSourceChangeAdd(row)));
  }
  return source;
}

type Fixture = {
  parent: TableSource;
  child: TableSource;
  fetch: (
    parentKey: CompoundKey,
    childKey: CompoundKey,
    req?: FetchRequest,
  ) => CaughtNode[];
};

// Most tests reuse the same org/membership shape: org has a UNIQUE INDEX on
// `slug`; membership joins to it via that slug.  Compound and PK-only cases
// override the schema as needed.
function orgFixture(opts?: {
  parentOrdering?: Ordering;
  childOrdering?: Ordering;
  extraOrgDDL?: string;
  parentRows?: readonly Row[];
  childRows?: readonly Row[];
}): Fixture {
  const db = new Database(createSilentLogContext(), ':memory:');
  db.exec(/* sql */ `
    CREATE TABLE org (id TEXT PRIMARY KEY, slug TEXT NOT NULL, name TEXT);
    CREATE UNIQUE INDEX org_slug_key ON org (slug);
    CREATE TABLE membership (
      id TEXT PRIMARY KEY,
      orgSlug TEXT NOT NULL,
      userId TEXT NOT NULL
    );
    ${opts?.extraOrgDDL ?? ''}
  `);

  const parent = makeSource(
    db,
    'org',
    {
      id: {type: 'string'},
      slug: {type: 'string'},
      name: {type: 'string'},
    },
    ['id'],
    opts?.parentRows ?? [
      {id: 'o1', slug: 'acme', name: 'Acme Inc'},
      {id: 'o2', slug: 'globex', name: 'Globex Corp'},
      {id: 'o3', slug: 'lonely', name: 'Lonely Org'},
    ],
  );
  const child = makeSource(
    db,
    'membership',
    {
      id: {type: 'string'},
      orgSlug: {type: 'string'},
      userId: {type: 'string'},
    },
    ['id'],
    opts?.childRows ?? [
      {id: 'm1', orgSlug: 'acme', userId: 'u1'},
      {id: 'm2', orgSlug: 'acme', userId: 'u2'},
      {id: 'm3', orgSlug: 'globex', userId: 'u3'},
      // Inner join drops m4 (no matching org).
      {id: 'm4', orgSlug: 'unknown', userId: 'u4'},
    ],
  );

  return {
    parent,
    child,
    fetch(parentKey, childKey, req) {
      const join = new FlippedJoin({
        parent: parent.connect(opts?.parentOrdering ?? [['id', 'asc']]),
        child: child.connect(opts?.childOrdering ?? [['id', 'asc']]),
        parentKey,
        childKey,
        relationshipName: 'memberships',
        hidden: false,
        system: 'client',
      });
      return new Catch(join).fetch(req);
    },
  };
}

const parentIds = (nodes: CaughtNode[]) =>
  nodes.map(n => (n as unknown as {row: {id: string}}).row.id);

const memberIds = (n: CaughtNode) =>
  (
    (n as unknown as {relationships: {memberships: CaughtNode[]}}).relationships
      .memberships ?? []
  ).map(c => (c as unknown as {row: {id: string}}).row.id);

test('parentKey === non-PK UNIQUE INDEX takes quicksort path with correct output', () => {
  const f = orgFixture();
  expect(f.parent.connect([['id', 'asc']]).getSchema().uniqueIndexes).toEqual(
    expect.arrayContaining([['id'], ['slug']]),
  );

  const result = f.fetch(['slug'], ['orgSlug']);

  expect(parentIds(result)).toEqual(['o1', 'o2']); // o3 dropped (no members)
  expect(memberIds(result[0])).toEqual(['m1', 'm2']); // child-input order
  expect(memberIds(result[1])).toEqual(['m3']);
});

test('parentKey === compound non-PK UNIQUE INDEX, columns in different order', () => {
  const db = new Database(createSilentLogContext(), ':memory:');
  db.exec(/* sql */ `
    CREATE TABLE project (
      id TEXT PRIMARY KEY,
      orgId TEXT NOT NULL,
      slug TEXT NOT NULL
    );
    CREATE UNIQUE INDEX project_org_slug_key ON project (orgId, slug);
    CREATE TABLE issue (
      id TEXT PRIMARY KEY,
      projectOrgId TEXT NOT NULL,
      projectSlug TEXT NOT NULL
    );
  `);
  const parent = makeSource(
    db,
    'project',
    {
      id: {type: 'string'},
      orgId: {type: 'string'},
      slug: {type: 'string'},
    },
    ['id'],
    [
      {id: 'p1', orgId: 'o1', slug: 'web'},
      {id: 'p2', orgId: 'o1', slug: 'api'},
      {id: 'p3', orgId: 'o2', slug: 'web'},
    ],
  );
  const child = makeSource(
    db,
    'issue',
    {
      id: {type: 'string'},
      projectOrgId: {type: 'string'},
      projectSlug: {type: 'string'},
    },
    ['id'],
    [
      {id: 'i1', projectOrgId: 'o1', projectSlug: 'web'},
      {id: 'i2', projectOrgId: 'o1', projectSlug: 'api'},
      {id: 'i3', projectOrgId: 'o2', projectSlug: 'web'},
    ],
  );

  // parentKey order ['slug', 'orgId'] differs from index order ['orgId', 'slug'].
  // keyMatchesPrimaryKey sorts both before comparing, so this still triggers
  // the quicksort path.
  const join = new FlippedJoin({
    parent: parent.connect([['id', 'asc']]),
    child: child.connect([['id', 'asc']]),
    parentKey: ['slug', 'orgId'],
    childKey: ['projectSlug', 'projectOrgId'],
    relationshipName: 'issues',
    hidden: false,
    system: 'client',
  });
  expect(parentIds(new Catch(join).fetch())).toEqual(['p1', 'p2', 'p3']);
});

test('parentKey not matching any unique index falls through to merge-sort, still correct', () => {
  const db = new Database(createSilentLogContext(), ':memory:');
  db.exec(/* sql */ `
    CREATE TABLE org (id TEXT PRIMARY KEY, region TEXT, name TEXT);
    CREATE TABLE membership (
      id TEXT PRIMARY KEY,
      orgRegion TEXT NOT NULL,
      userId TEXT NOT NULL
    );
  `);
  const parent = makeSource(
    db,
    'org',
    {
      id: {type: 'string'},
      region: {type: 'string'},
      name: {type: 'string'},
    },
    ['id'],
    [
      // Two orgs share region 'us' — region is NOT unique.
      {id: 'o1', region: 'us', name: 'Org A'},
      {id: 'o2', region: 'us', name: 'Org B'},
      {id: 'o3', region: 'eu', name: 'Org C'},
    ],
  );
  const child = makeSource(
    db,
    'membership',
    {
      id: {type: 'string'},
      orgRegion: {type: 'string'},
      userId: {type: 'string'},
    },
    ['id'],
    [{id: 'm1', orgRegion: 'us', userId: 'u1'}],
  );

  // Schema must NOT advertise `region` as unique.
  expect(parent.connect([['id', 'asc']]).getSchema().uniqueIndexes).toEqual([
    ['id'],
  ]);

  const join = new FlippedJoin({
    parent: parent.connect([['id', 'asc']]),
    child: child.connect([['id', 'asc']]),
    parentKey: ['region'],
    childKey: ['orgRegion'],
    relationshipName: 'memberships',
    hidden: false,
    system: 'client',
  });
  const result = new Catch(join).fetch();

  // Merge-sort path: m1 has two matching parents (o1, o2), both included.
  expect(parentIds(result)).toEqual(['o1', 'o2']);
});

test('quicksort path respects req.reverse', () => {
  const f = orgFixture();
  expect(parentIds(f.fetch(['slug'], ['orgSlug'], {reverse: true}))).toEqual([
    'o2',
    'o1',
  ]);
});

test('quicksort path respects req.start basis "at" and "after"', () => {
  const f = orgFixture();

  expect(
    parentIds(
      f.fetch(['slug'], ['orgSlug'], {
        start: {row: {id: 'o2'}, basis: 'at'},
      }),
    ),
  ).toEqual(['o2']); // o3 has no members so it's dropped anyway

  expect(
    parentIds(
      f.fetch(['slug'], ['orgSlug'], {
        start: {row: {id: 'o1'}, basis: 'after'},
      }),
    ),
  ).toEqual(['o2']);

  // Reverse + start
  expect(
    parentIds(
      f.fetch(['slug'], ['orgSlug'], {
        start: {row: {id: 'o2'}, basis: 'at'},
        reverse: true,
      }),
    ),
  ).toEqual(['o2', 'o1']);
});

test('quicksort path propagates req.constraint to per-child fetch', () => {
  const f = orgFixture();
  // Constrain to org id=o1.  parentKey is ['slug'], so id=o1 is NOT a parentKey
  // column — it goes through unchanged via {...req.constraint, ...}
  // and the per-child parent fetch must intersect both.
  const result = f.fetch(['slug'], ['orgSlug'], {constraint: {id: 'o1'}});
  expect(parentIds(result)).toEqual(['o1']);
  expect(memberIds(result[0])).toEqual(['m1', 'm2']);
});

test('multiple children sharing a parent are returned in child-input order', () => {
  // Sanity check on the group-by-parent logic in #fetchQuicksort.  Children
  // are inserted in 'm1', 'm2' order and the child ordering is by id asc.
  const f = orgFixture({
    childRows: [
      {id: 'm2', orgSlug: 'acme', userId: 'u2'},
      {id: 'm1', orgSlug: 'acme', userId: 'u1'},
      {id: 'm3', orgSlug: 'acme', userId: 'u3'},
    ],
  });
  const result = f.fetch(['slug'], ['orgSlug']);
  expect(parentIds(result)).toEqual(['o1']);
  // child connect ordering is id asc, so the relationship is m1, m2, m3.
  expect(memberIds(result[0])).toEqual(['m1', 'm2', 'm3']);
});
