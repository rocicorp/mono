import {expect, test} from 'vitest';
import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../zero-schema/src/builder/table-builder.ts';
import {newQuery} from './query-impl.ts';
import {asQueryInternals} from './query-internals.ts';

/**
 * `{scalar: true}` is only honored by the server when the subquery is provably
 * limited to one row — every column of the primary key, or of a declared
 * {@linkcode TableBuilderWithColumns.unique} key, constrained to a literal by
 * `=`. Anything else silently degrades to a plain `EXISTS`, so the type
 * rejects it. These cases pin down where that line falls.
 */

const project = table('project')
  .columns({
    id: string(),
    name: string(),
    lowerCaseName: string(),
  })
  .primaryKey('id')
  // Mirrors `CREATE UNIQUE INDEX ... ON project (lowerCaseName)` upstream.
  .unique('lowerCaseName');

const label = table('label')
  .columns({
    id: string(),
    projectID: string(),
    name: string(),
  })
  .primaryKey('id')
  .unique('projectID', 'name');

const issueLabel = table('issueLabel')
  .columns({
    issueID: string(),
    labelID: string(),
  })
  .primaryKey('issueID', 'labelID');

const issue = table('issue')
  .columns({
    id: string(),
    projectID: string(),
    title: string(),
  })
  .primaryKey('id');

const issueRelationships = relationships(issue, ({one}) => ({
  project: one({
    sourceField: ['projectID'],
    destField: ['id'],
    destSchema: project,
  }),
}));

const issueLabelRelationships = relationships(issueLabel, ({one}) => ({
  label: one({
    sourceField: ['labelID'],
    destField: ['id'],
    destSchema: label,
  }),
}));

const labelRelationships = relationships(label, ({one}) => ({
  project: one({
    sourceField: ['projectID'],
    destField: ['id'],
    destSchema: project,
  }),
}));

const schema = createSchema({
  tables: [issue, issueLabel, label, project],
  relationships: [
    issueRelationships,
    issueLabelRelationships,
    labelRelationships,
  ],
});

test('primary key pinned by = is accepted', () => {
  const q = newQuery(schema, 'issue').whereExists(
    'project',
    p => p.where('id', '=', 'p1'),
    {scalar: true},
  );
  expect(asQueryInternals(q).ast.where).toMatchObject({scalar: true});
});

test('the implicit-= form pins too', () => {
  const q = newQuery(schema, 'issue').whereExists(
    'project',
    p => p.where('id', 'p1'),
    {scalar: true},
  );
  expect(asQueryInternals(q).ast.where).toMatchObject({scalar: true});
});

test('a declared unique key is accepted', () => {
  // `lowerCaseName` is not the primary key; only the `.unique()` declaration
  // makes this provable on the client. This is the zbugs shape.
  const q = newQuery(schema, 'issue').whereExists(
    'project',
    p => p.where('lowerCaseName', '=', 'zero'),
    {scalar: true},
  );
  expect(asQueryInternals(q).ast.where).toMatchObject({scalar: true});
});

test('a compound unique key needs every column', () => {
  const q = newQuery(schema, 'label').whereExists(
    'project',
    p => p.where('id', '=', 'p1'),
    {scalar: true},
  );
  expect(asQueryInternals(q).ast.where).toMatchObject({scalar: true});
});

test('pinning survives chaining in any order', () => {
  const q = newQuery(schema, 'issue').whereExists(
    'project',
    p => p.where('name', 'LIKE', 'z%').where('id', '=', 'p1').limit(5),
    {scalar: true},
  );
  expect(asQueryInternals(q).ast.where).toMatchObject({scalar: true});
});

test('an inner scalar gate completes an outer compound key', () => {
  // `label`'s unique key is (projectID, name) and only `name` is pinned here.
  // The inner `project` gate is itself scalar, so the server rewrites it to
  // `label.projectID = <literal>` before testing this gate — completing the
  // key. The type follows the same rule.
  // (Regression-tested end to end in
  // zqlite/src/resolve-nested-scalar.test.ts.)
  const q = newQuery(schema, 'issueLabel').whereExists(
    'label',
    l =>
      l
        .where('name', '=', 'bug')
        .whereExists('project', p => p.where('lowerCaseName', '=', 'zero'), {
          scalar: true,
        }),
    {scalar: true},
  );
  expect(asQueryInternals(q).ast.where).toMatchObject({scalar: true});
});

test('a non-scalar inner gate does not complete the key', () => {
  // Same shape without the inner hint: nothing supplies `projectID`, so the
  // outer gate is genuinely unpinned.
  const q = newQuery(schema, 'issueLabel').whereExists(
    'label',
    l =>
      l
        .where('name', '=', 'bug')
        .whereExists('project', p => p.where('lowerCaseName', '=', 'zero')),
    {
      // @ts-expect-error `name` alone does not cover (projectID, name)
      scalar: true,
    },
  );
  expect(asQueryInternals(q).ast.where).toMatchObject({scalar: true});
});

test('scalar: false is always allowed', () => {
  const q = newQuery(schema, 'issue').whereExists('project', p => p, {
    scalar: false,
  });
  expect(asQueryInternals(q).ast.where).toMatchObject({scalar: false});
});

test('flip is unaffected by pinning', () => {
  const q = newQuery(schema, 'issue').whereExists('project', p => p, {
    flip: true,
  });
  expect(asQueryInternals(q).ast.where).toMatchObject({flip: true});
});

test('rejected shapes are type errors but still build at runtime', () => {
  // The directive sits on the `scalar` property because that is where the
  // overload mismatch is reported.
  // Runtime behavior is unchanged — the server simply ignores the hint.

  const unpinned = newQuery(schema, 'issue').whereExists('project', p => p, {
    // @ts-expect-error the subquery pins nothing
    scalar: true,
  });
  expect(asQueryInternals(unpinned).ast.where).toMatchObject({scalar: true});

  const nonUnique = newQuery(schema, 'issue').whereExists(
    'project',
    p => p.where('name', '=', 'Zero'),
    {
      // @ts-expect-error `name` is not unique on `project`
      scalar: true,
    },
  );
  expect(asQueryInternals(nonUnique).ast.where).toMatchObject({scalar: true});

  const nonEquality = newQuery(schema, 'issue').whereExists(
    'project',
    p => p.where('id', 'LIKE', 'p%'),
    {
      // @ts-expect-error LIKE does not pin `id` to a literal
      scalar: true,
    },
  );
  expect(asQueryInternals(nonEquality).ast.where).toMatchObject({scalar: true});

  const partial = newQuery(schema, 'label').whereExists(
    'project',
    p => p.where('name', '=', 'zero'),
    {
      // @ts-expect-error `name` covers no unique key of `project`
      scalar: true,
    },
  );
  expect(asQueryInternals(partial).ast.where).toMatchObject({scalar: true});

  const noCallback = newQuery(schema, 'issue').whereExists('project', {
    // @ts-expect-error with no callback nothing can be pinned
    scalar: true,
  });
  expect(asQueryInternals(noCallback).ast.where).toMatchObject({scalar: true});
});

test('the rejections above are attributable to `scalar`, not to the query', () => {
  // Controls for each rejected shape: the identical call *without* the option
  // must compile. `@ts-expect-error` swallows whatever error lands on its line,
  // so without these a case could go on "passing" after it started failing for
  // an unrelated reason — a renamed column, a dropped relationship.
  const q = newQuery(schema, 'issue');
  expect(q.whereExists('project', p => p)).toBeDefined();
  expect(
    q.whereExists('project', p => p.where('name', '=', 'Zero')),
  ).toBeDefined();
  expect(
    q.whereExists('project', p => p.where('id', 'LIKE', 'p%')),
  ).toBeDefined();
  expect(
    newQuery(schema, 'label').whereExists('project', p =>
      p.where('name', '=', 'zero'),
    ),
  ).toBeDefined();
  expect(q.whereExists('project')).toBeDefined();
  expect(
    newQuery(schema, 'issueLabel').whereExists('label', l =>
      l
        .where('name', '=', 'bug')
        .whereExists('project', p => p.where('lowerCaseName', '=', 'zero')),
    ),
  ).toBeDefined();
});
