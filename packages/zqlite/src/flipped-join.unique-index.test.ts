import {expect, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {Catch} from '../../zql/src/ivm/catch.ts';
import {FlippedJoin} from '../../zql/src/ivm/flipped-join.ts';
import {makeSourceChangeAdd} from '../../zql/src/ivm/source.ts';
import {consume} from '../../zql/src/ivm/stream.ts';
import {Database} from './db.ts';
import {TableSource} from './table-source.ts';

const lc = createSilentLogContext();

// Exercises the #fetchQuicksort path through a parentKey that matches a
// non-primary UNIQUE INDEX.  This is the only branch in #parentKeyIsUnique
// that depends on the schema.uniqueIndexes plumbing — MemorySource doesn't
// populate it, so this path is only reachable via TableSource.
test('flipped-join uses quicksort path when parentKey matches non-PK UNIQUE INDEX', () => {
  const db = new Database(createSilentLogContext(), ':memory:');
  db.exec(/* sql */ `
    CREATE TABLE org (id TEXT PRIMARY KEY, slug TEXT NOT NULL, name TEXT);
    CREATE UNIQUE INDEX org_slug_key ON org (slug);
    CREATE TABLE membership (
      id TEXT PRIMARY KEY,
      orgSlug TEXT NOT NULL,
      userId TEXT NOT NULL
    );
  `);

  const orgSource = new TableSource(
    lc,
    testLogConfig,
    db,
    'org',
    {
      id: {type: 'string'},
      slug: {type: 'string'},
      name: {type: 'string'},
    },
    ['id'],
  );
  const membershipSource = new TableSource(
    lc,
    testLogConfig,
    db,
    'membership',
    {
      id: {type: 'string'},
      orgSlug: {type: 'string'},
      userId: {type: 'string'},
    },
    ['id'],
  );

  for (const row of [
    {id: 'o1', slug: 'acme', name: 'Acme Inc'},
    {id: 'o2', slug: 'globex', name: 'Globex Corp'},
    {id: 'o3', slug: 'lonely', name: 'Lonely Org'},
  ]) {
    consume(orgSource.push(makeSourceChangeAdd(row)));
  }
  for (const row of [
    {id: 'm1', orgSlug: 'acme', userId: 'u1'},
    {id: 'm2', orgSlug: 'acme', userId: 'u2'},
    {id: 'm3', orgSlug: 'globex', userId: 'u3'},
    // m4 references slug 'unknown' which doesn't exist — inner join drops it.
    {id: 'm4', orgSlug: 'unknown', userId: 'u4'},
  ]) {
    consume(membershipSource.push(makeSourceChangeAdd(row)));
  }

  // parentKey is ['slug'], which matches the non-PK unique index.
  // Without uniqueIndexes plumbing this would fall through to merge-sort.
  const parent = orgSource.connect([['id', 'asc']]);
  const child = membershipSource.connect([['id', 'asc']]);

  expect(parent.getSchema().uniqueIndexes).toEqual(
    expect.arrayContaining([['id'], ['slug']]),
  );

  const flippedJoin = new FlippedJoin({
    parent,
    child,
    parentKey: ['slug'],
    childKey: ['orgSlug'],
    relationshipName: 'memberships',
    hidden: false,
    system: 'client',
  });

  const c = new Catch(flippedJoin);
  const result = c.fetch();

  // Inner join: o3 (no memberships) is dropped, m4 (no org) is dropped.
  // Children within a parent stay in child-input order due to stable sort.
  expect(result).toEqual([
    {
      row: {id: 'o1', slug: 'acme', name: 'Acme Inc'},
      relationships: {
        memberships: [
          {
            row: {id: 'm1', orgSlug: 'acme', userId: 'u1'},
            relationships: {},
          },
          {
            row: {id: 'm2', orgSlug: 'acme', userId: 'u2'},
            relationships: {},
          },
        ],
      },
    },
    {
      row: {id: 'o2', slug: 'globex', name: 'Globex Corp'},
      relationships: {
        memberships: [
          {
            row: {id: 'm3', orgSlug: 'globex', userId: 'u3'},
            relationships: {},
          },
        ],
      },
    },
  ]);
});

test('flipped-join uses quicksort path when parentKey matches compound non-PK UNIQUE INDEX', () => {
  const db = new Database(createSilentLogContext(), ':memory:');
  db.exec(/* sql */ `
    CREATE TABLE project (
      id TEXT PRIMARY KEY,
      orgId TEXT NOT NULL,
      slug TEXT NOT NULL,
      name TEXT
    );
    CREATE UNIQUE INDEX project_org_slug_key ON project (orgId, slug);
    CREATE TABLE issue (
      id TEXT PRIMARY KEY,
      projectOrgId TEXT NOT NULL,
      projectSlug TEXT NOT NULL,
      title TEXT
    );
  `);

  const projectSource = new TableSource(
    lc,
    testLogConfig,
    db,
    'project',
    {
      id: {type: 'string'},
      orgId: {type: 'string'},
      slug: {type: 'string'},
      name: {type: 'string'},
    },
    ['id'],
  );
  const issueSource = new TableSource(
    lc,
    testLogConfig,
    db,
    'issue',
    {
      id: {type: 'string'},
      projectOrgId: {type: 'string'},
      projectSlug: {type: 'string'},
      title: {type: 'string'},
    },
    ['id'],
  );

  for (const row of [
    {id: 'p1', orgId: 'o1', slug: 'web', name: 'Web'},
    {id: 'p2', orgId: 'o1', slug: 'api', name: 'API'},
    {id: 'p3', orgId: 'o2', slug: 'web', name: 'Web'},
  ]) {
    consume(projectSource.push(makeSourceChangeAdd(row)));
  }
  for (const row of [
    {id: 'i1', projectOrgId: 'o1', projectSlug: 'web', title: 'a'},
    {id: 'i2', projectOrgId: 'o1', projectSlug: 'api', title: 'b'},
    {id: 'i3', projectOrgId: 'o2', projectSlug: 'web', title: 'c'},
  ]) {
    consume(issueSource.push(makeSourceChangeAdd(row)));
  }

  const parent = projectSource.connect([['id', 'asc']]);
  const child = issueSource.connect([['id', 'asc']]);

  const flippedJoin = new FlippedJoin({
    parent,
    child,
    // parentKey order need not match index column order — keyMatchesPrimaryKey
    // sorts before comparing.
    parentKey: ['slug', 'orgId'],
    childKey: ['projectSlug', 'projectOrgId'],
    relationshipName: 'issues',
    hidden: false,
    system: 'client',
  });

  const c = new Catch(flippedJoin);
  const result = c.fetch();

  expect(result.map(n => (n as unknown as {row: {id: string}}).row.id)).toEqual(
    ['p1', 'p2', 'p3'],
  );
});
