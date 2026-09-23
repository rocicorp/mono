// A flipped EXISTS branch under an OR tags each row it emits with the child
// rows that made it pass (its witnesses). zero-cache syncs those witnesses by
// refcounting every row in an add/remove subtree (pipeline-driver's Streamer
// and the CVR), so the union fan-in's incremental change stream has to account
// for witnesses exactly the way a fresh hydration does. These tests fold both
// the way zero-cache does and compare.
import {describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {must} from '../../../shared/src/must.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {buildPipeline} from '../builder/builder.ts';
import {TestBuilderDelegate} from '../builder/test-builder-delegate.ts';
import {newQuery} from '../query/query-impl.ts';
import {asQueryInternals} from '../query/query-internals.ts';
import {Catch, type CaughtChange, type CaughtNode} from './catch.ts';
import type {SourceSchema} from './schema.ts';
import type {Source, SourceChange} from './source.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
} from './source.ts';
import {consume} from './stream.ts';
import {createSource} from './test/source-factory.ts';

const lc = createSilentLogContext();

/**
 * zero-cache's per-query row refcounts, fed the way `Streamer` feeds them:
 * every row in an add or remove subtree is counted, a child change recurses
 * into its subtree, and an edit touches no counts.
 */
class Footprint {
  readonly #counts = new Map<string, number>();

  #node(schema: SourceSchema, node: CaughtNode, delta: 1 | -1): void {
    if (node === 'yield') {
      return;
    }
    const key = `${schema.tableName}:${schema.primaryKey.map(k => node.row[k]).join('/')}`;
    this.#counts.set(key, (this.#counts.get(key) ?? 0) + delta);
    for (const [name, children] of Object.entries(node.relationships)) {
      const childSchema = must(schema.relationships[name]);
      for (const child of children) {
        this.#node(childSchema, child, delta);
      }
    }
  }

  hydrate(schema: SourceSchema, nodes: CaughtNode[]): void {
    for (const node of nodes) {
      this.#node(schema, node, 1);
    }
  }

  apply(schema: SourceSchema, change: CaughtChange): void {
    switch (change.type) {
      case 'add':
        this.#node(schema, change.node, 1);
        break;
      case 'remove':
        this.#node(schema, change.node, -1);
        break;
      case 'child':
        this.apply(
          must(schema.relationships[change.child.relationshipName]),
          change.child.change,
        );
        break;
      case 'edit':
        break;
    }
  }

  /** Every row with a non-zero count. A negative count is a bug too. */
  counts(): string[] {
    return [...this.#counts]
      .filter(([, n]) => n !== 0)
      .map(([k, n]) => `${k}=${n}`)
      .sort();
  }
}

type Tables = Record<
  string,
  {columns: SourceSchema['columns']; primaryKey: SourceSchema['primaryKey']}
>;

function makeSources(
  tables: Tables,
  data: Record<string, Row[]>,
): Record<string, Source> {
  const sources: Record<string, Source> = {};
  for (const [name, {columns, primaryKey}] of Object.entries(tables)) {
    const source = createSource(lc, testLogConfig, name, columns, primaryKey);
    for (const row of data[name] ?? []) {
      consume(source.push(makeSourceChangeAdd(row)));
    }
    sources[name] = source;
  }
  return sources;
}

/**
 * Hydrates `ast` over `data`, applies `pushes`, and returns the refcounts a
 * client would end up holding.
 */
function run(
  tables: Tables,
  ast: AST,
  data: Record<string, Row[]>,
  pushes: [table: string, change: SourceChange][] = [],
): string[] {
  const sources = makeSources(tables, data);
  const input = buildPipeline(ast, new TestBuilderDelegate(sources), 'q');
  const schema = input.getSchema();
  const c = new Catch(input);
  const fp = new Footprint();
  fp.hydrate(schema, c.fetch());
  for (const [name, change] of pushes) {
    consume(must(sources[name]).push(change));
  }
  for (const push of c.pushes) {
    fp.apply(schema, push);
  }
  return fp.counts();
}

describe('or(filter, flipped exists) over issue/project', () => {
  const project = table('project')
    .columns({id: string(), active: boolean()})
    .primaryKey('id');
  const issue = table('issue')
    .columns({
      id: number(),
      assignee: string().optional(),
      priority: string().optional(),
      projectID: string(),
    })
    .primaryKey('id');
  const schema = createSchema({
    tables: [project, issue],
    relationships: [
      relationships(issue, ({one}) => ({
        project: one({
          sourceField: ['projectID'],
          destField: ['id'],
          destSchema: project,
        }),
      })),
    ],
  });
  const tables: Tables = {
    project: schema.tables.project,
    issue: schema.tables.issue,
  };

  // Unassigned issues, plus every issue in an active project.
  const unassignedOrActive = asQueryInternals(
    newQuery(schema, 'issue').where(({or, cmp, exists}) =>
      or(
        cmp('assignee', 'IS', null),
        exists('project', p => p.where('active', true), {flip: true}),
      ),
    ),
  ).ast;

  // Unassigned issues, plus high-priority issues in an active project.
  const unassignedOrHighInActive = asQueryInternals(
    newQuery(schema, 'issue').where(({or, and, cmp, exists}) =>
      or(
        cmp('assignee', 'IS', null),
        and(
          cmp('priority', 'high'),
          exists('project', p => p.where('active', true), {flip: true}),
        ),
      ),
    ),
  ).ast;

  const p1 = {id: 'p1', active: true};
  // Passes both branches.
  const i1 = {id: 1, assignee: null, projectID: 'p1'};
  // Passes the exists branch only.
  const i2 = {id: 2, assignee: 'ann', projectID: 'p1'};

  test('removing a row held by both branches keeps the witness other rows still need', () => {
    const incremental = run(
      tables,
      unassignedOrActive,
      {project: [p1], issue: [i1, i2]},
      [['issue', makeSourceChangeRemove(i1)]],
    );
    const fresh = run(tables, unassignedOrActive, {project: [p1], issue: [i2]});
    expect(incremental).toEqual(fresh);
    expect(incremental).toContain('project:p1=1');
  });

  test('a witness that stops matching leaves even though another branch keeps the row', () => {
    const archived = {id: 'p1', active: false};
    const incremental = run(
      tables,
      unassignedOrActive,
      {project: [p1], issue: []},
      [
        ['issue', makeSourceChangeAdd(i1)],
        ['project', makeSourceChangeEdit(archived, p1)],
      ],
    );
    const fresh = run(tables, unassignedOrActive, {
      project: [archived],
      issue: [i1],
    });
    expect(incremental).toEqual(fresh);
    expect(incremental).toEqual(['issue:1=1']);
  });

  test('a row that moves from the filter branch into the exists branch gains its witness', () => {
    const assigned = {...i1, assignee: 'ann'};
    const incremental = run(
      tables,
      unassignedOrActive,
      {project: [p1], issue: [i1]},
      [['issue', makeSourceChangeEdit(assigned, i1)]],
    );
    const fresh = run(tables, unassignedOrActive, {
      project: [p1],
      issue: [assigned],
    });
    expect(incremental).toEqual(fresh);
    expect(incremental).toContain('project:p1=1');
  });

  test('a row that enters a flipped branch while the filter branch holds it, then leaves the filter branch', () => {
    const low = {id: 1, assignee: null, priority: 'low', projectID: 'p1'};
    const high = {...low, priority: 'high'};
    const assigned = {...high, assignee: 'ann'};
    const incremental = run(
      tables,
      unassignedOrHighInActive,
      {project: [p1], issue: [low]},
      [
        ['issue', makeSourceChangeEdit(high, low)],
        ['issue', makeSourceChangeEdit(assigned, high)],
      ],
    );
    const fresh = run(tables, unassignedOrHighInActive, {
      project: [p1],
      issue: [assigned],
    });
    expect(incremental).toEqual(fresh);
    expect(incremental).toContain('project:p1=1');
  });
});

describe('or(flipped exists, filter) on a self-join with related and limit', () => {
  const tables: Tables = {
    employee: {
      columns: {
        id: {type: 'number'},
        name: {type: 'string'},
        reportsTo: {type: 'number', optional: true},
      },
      primaryKey: ['id'],
    },
  };

  // employee WHERE EXISTS(reports, flip) OR reportsTo IS NULL
  //   .related(manager) ORDER BY reportsTo, id LIMIT 2
  const ast: AST = {
    table: 'employee',
    orderBy: [
      ['reportsTo', 'asc'],
      ['id', 'asc'],
    ],
    limit: 2,
    where: {
      type: 'or',
      conditions: [
        {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          flip: true,
          related: {
            system: 'client',
            correlation: {parentField: ['id'], childField: ['reportsTo']},
            subquery: {
              table: 'employee',
              alias: 'reports',
              orderBy: [['id', 'asc']],
            },
          },
        },
        {
          type: 'simple',
          op: 'IS',
          left: {type: 'column', name: 'reportsTo'},
          right: {type: 'literal', value: null},
        },
      ],
    },
    related: [
      {
        system: 'client',
        correlation: {parentField: ['reportsTo'], childField: ['id']},
        subquery: {
          table: 'employee',
          alias: 'manager',
          orderBy: [['id', 'asc']],
        },
      },
    ],
  };

  const e1 = {id: 1, name: 'Adams', reportsTo: null};
  const e2 = {id: 2, name: 'Mills', reportsTo: 1};
  const e3 = {id: 3, name: 'Park', reportsTo: 2};

  test('hydrate and push agree on the witness subtree of a row in both branches', () => {
    // e1 passes both branches: reportsTo IS NULL, and e2 reports to it.
    const incremental = run(tables, ast, {employee: [e1, e2, e3]}, [
      ['employee', makeSourceChangeRemove(e1)],
    ]);
    const fresh = run(tables, ast, {employee: [e2, e3]});
    expect(incremental).toEqual(fresh);
  });

  test('the last witness leaving is not dropped because another branch holds the row', () => {
    const e4 = {id: 4, name: 'Witness', reportsTo: 1};
    const incremental = run(tables, ast, {employee: [e4]}, [
      ['employee', makeSourceChangeAdd(e1)],
      ['employee', makeSourceChangeRemove(e4)],
    ]);
    const fresh = run(tables, ast, {employee: [e1]});
    expect(incremental).toEqual(fresh);
  });
});

describe('a flipped exists in a union nested inside another union', () => {
  const project = table('project').columns({id: string()}).primaryKey('id');
  const owner = table('owner').columns({id: string()}).primaryKey('id');
  const issue = table('issue')
    .columns({
      id: number(),
      x: number(),
      y: number(),
      projectID: string(),
      ownerID: string(),
    })
    .primaryKey('id');
  const schema = createSchema({
    tables: [project, owner, issue],
    relationships: [
      relationships(issue, ({one}) => ({
        project: one({
          sourceField: ['projectID'],
          destField: ['id'],
          destSchema: project,
        }),
        owner: one({
          sourceField: ['ownerID'],
          destField: ['id'],
          destSchema: owner,
        }),
      })),
    ],
  });
  const tables: Tables = {
    project: schema.tables.project,
    owner: schema.tables.owner,
    issue: schema.tables.issue,
  };

  // y=1 OR (y>=0 AND (x=0 OR (x=1 AND EXISTS(project, flip))))
  // A row with y=1 is in both outer branches.
  const nested = asQueryInternals(
    newQuery(schema, 'issue').where(({or, and, cmp, exists}) =>
      or(
        cmp('y', 1),
        and(
          cmp('y', '>=', 0),
          or(
            cmp('x', 0),
            and(
              cmp('x', 1),
              exists('project', p => p, {flip: true}),
            ),
          ),
        ),
      ),
    ),
  ).ast;

  const p1 = {id: 'p1'};
  const o1 = {id: 'o1'};
  const base = {id: 1, x: 0, y: 0, projectID: 'p1', ownerID: 'o1'};

  function editCase(before: Row, after: Row) {
    const incremental = run(tables, nested, {project: [p1], issue: [before]}, [
      ['issue', makeSourceChangeEdit(after, before)],
    ]);
    const fresh = run(tables, nested, {project: [p1], issue: [after]});
    expect(incremental).toEqual(fresh);
    return incremental;
  }

  test('an edit that moves the row into the inner flipped branch brings its witness', () => {
    expect(editCase(base, {...base, x: 1})).toEqual([
      'issue:1=1',
      'project:p1=1',
    ]);
  });

  test('an edit that moves the row out of the inner flipped branch takes its witness', () => {
    expect(editCase({...base, x: 1}, base)).toEqual(['issue:1=1']);
  });

  test('an edit that leaves one outer branch while entering the inner flipped branch', () => {
    expect(editCase({...base, y: 1}, {...base, x: 1})).toEqual([
      'issue:1=1',
      'project:p1=1',
    ]);
  });

  test('an edit that enters one outer branch while leaving the inner flipped branch', () => {
    expect(editCase({...base, x: 1}, {...base, y: 1})).toEqual(['issue:1=1']);
  });

  // y=1 OR (y>=0 AND (x=0 OR (EXISTS(owner) AND EXISTS(project, flip))))
  // The owner join sits above both unions, so a new owner reaches them as a
  // child change, which the inner EXISTS(owner) filter turns into an add.
  const nestedOwned = asQueryInternals(
    newQuery(schema, 'issue').where(({or, and, cmp, exists}) =>
      or(
        cmp('y', 1),
        and(
          cmp('y', '>=', 0),
          or(
            cmp('x', 0),
            and(
              exists('owner'),
              exists('project', p => p, {flip: true}),
            ),
          ),
        ),
      ),
    ),
  ).ast;

  test.each([
    ['in the inner union only', base],
    ['in both outer branches', {...base, y: 1}],
  ])(
    'a child change that moves the row into the inner flipped branch brings its witness (%s)',
    (_, row) => {
      const incremental = run(
        tables,
        nestedOwned,
        {project: [p1], issue: [row]},
        [['owner', makeSourceChangeAdd(o1)]],
      );
      const fresh = run(tables, nestedOwned, {
        project: [p1],
        owner: [o1],
        issue: [row],
      });
      expect(incremental).toEqual(fresh);
      expect(incremental).toContain('project:p1=1');
    },
  );

  test.each([
    ['in the inner union only', base],
    ['in both outer branches', {...base, y: 1}],
  ])(
    'a child change that moves the row out of the inner flipped branch takes its witness (%s)',
    (_, row) => {
      const incremental = run(
        tables,
        nestedOwned,
        {project: [p1], owner: [o1], issue: [row]},
        [['owner', makeSourceChangeRemove(o1)]],
      );
      const fresh = run(tables, nestedOwned, {project: [p1], issue: [row]});
      expect(incremental).toEqual(fresh);
      expect(incremental).not.toContain('project:p1=1');
    },
  );
});
