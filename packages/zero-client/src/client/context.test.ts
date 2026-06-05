import {LogContext} from '@rocicorp/logger';
import {expect, test} from 'vitest';
import type {NoIndexDiff} from '../../../replicache/src/btree/node.ts';
import type {Hash} from '../../../replicache/src/hash.ts';
import {assert} from '../../../shared/src/asserts.ts';
import type {AST, CorrelatedSubquery} from '../../../zero-protocol/src/ast.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../zero-schema/src/builder/table-builder.ts';
import {
  aggregateTableName,
  buildPipeline,
  topLevelAggregateTableName,
} from '../../../zql/src/builder/builder.ts';
import {ArrayView} from '../../../zql/src/ivm/array-view.ts';
import {Catch} from '../../../zql/src/ivm/catch.ts';
import {defaultFormat} from '../../../zql/src/ivm/default-format.ts';
import {Join} from '../../../zql/src/ivm/join.ts';
import {MemorySource} from '../../../zql/src/ivm/memory-source.ts';
import {
  ZeroContext,
  type AddCustomQuery,
  type AddQuery,
  type FlushQueryChanges,
  type UpdateCustomQuery,
  type UpdateQuery,
} from './context.ts';
import {IVMSourceBranch} from './ivm-branch.ts';
import {ENTITIES_KEY_PREFIX} from './keys.ts';
const testBatchViewUpdates = (applyViewUpdates: () => void) =>
  applyViewUpdates();

function assertValidRunOptions(): void {}

test('getSource', () => {
  const schema = createSchema({
    tables: [
      table('users')
        .columns({
          id: string(),
          name: string(),
        })
        .primaryKey('id'),
      table('userStates')
        .columns({
          userID: string(),
          stateCode: string(),
        })
        .primaryKey('userID', 'stateCode'),
    ],
  });

  const context = new ZeroContext(
    new LogContext('info'),
    new IVMSourceBranch(schema.tables),
    null as unknown as AddQuery,
    null as unknown as AddCustomQuery,
    null as unknown as UpdateQuery,
    null as unknown as UpdateCustomQuery,
    null as unknown as FlushQueryChanges,
    testBatchViewUpdates,
    () => {},
    assertValidRunOptions,
  );

  const source = context.getSource('users');
  assert(
    source instanceof MemorySource,
    'Expected source to be a MemorySource instance',
  );
  expect(source.tableSchema).toMatchInlineSnapshot(`
    {
      "columns": {
        "id": {
          "customType": null,
          "optional": false,
          "type": "string",
        },
        "name": {
          "customType": null,
          "optional": false,
          "type": "string",
        },
      },
      "name": "users",
      "primaryKey": [
        "id",
      ],
    }
  `);

  // Calling again should cache first value.
  expect(context.getSource('users')).toBe(source);

  expect(context.getSource('nonexistent')).toBeUndefined();

  // Should work for other table too.
  const source2 = context.getSource('userStates');
  expect((source2 as MemorySource).tableSchema).toMatchInlineSnapshot(`
    {
      "columns": {
        "stateCode": {
          "customType": null,
          "optional": false,
          "type": "string",
        },
        "userID": {
          "customType": null,
          "optional": false,
          "type": "string",
        },
      },
      "name": "userStates",
      "primaryKey": [
        "userID",
        "stateCode",
      ],
    }
  `);
});
const schema = createSchema({
  tables: [
    table('t1')
      .columns({
        id: string(),
        name: string(),
      })
      .primaryKey('id'),
  ],
});

test('processChanges', () => {
  const context = new ZeroContext(
    new LogContext('info'),
    new IVMSourceBranch(schema.tables),
    null as unknown as AddQuery,
    null as unknown as AddCustomQuery,
    null as unknown as UpdateQuery,
    null as unknown as UpdateCustomQuery,
    null as unknown as FlushQueryChanges,
    testBatchViewUpdates,
    () => {},
    assertValidRunOptions,
  );
  const out = new Catch(
    // oxlint-disable-next-line no-non-null-assertion
    context.getSource('t1')!.connect([
      ['name', 'desc'],
      ['id', 'desc'],
    ]),
  );

  context.processChanges(undefined, 'ahash' as Hash, [
    {
      key: `${ENTITIES_KEY_PREFIX}t1/e1`,
      op: 'add',
      newValue: {id: 'e1', name: 'name1'},
    },
    {
      key: `${ENTITIES_KEY_PREFIX}t1/e2`,
      op: 'add',
      newValue: {id: 'e2', name: 'name2'},
    },
    {
      key: `${ENTITIES_KEY_PREFIX}t1/e1`,
      op: 'change',
      oldValue: {id: 'e1', name: 'name1'},
      newValue: {id: 'e1', name: 'name1.1'},
    },
  ]);

  expect(out.pushes).toEqual([
    {type: 'add', node: {row: {id: 'e1', name: 'name1'}, relationships: {}}},
    {type: 'add', node: {row: {id: 'e2', name: 'name2'}, relationships: {}}},
    {
      type: 'edit',
      row: {id: 'e1', name: 'name1.1'},
      oldRow: {id: 'e1', name: 'name1'},
    },
  ]);

  expect(out.fetch({})).toEqual([
    {row: {id: 'e2', name: 'name2'}, relationships: {}},
    {row: {id: 'e1', name: 'name1.1'}, relationships: {}},
  ]);
});

test('processChanges wraps source updates with batchViewUpdates', () => {
  let batchViewUpdatesCalls = 0;
  const batchViewUpdates = (applyViewUpdates: () => void) => {
    batchViewUpdatesCalls++;
    expect(out.pushes).toEqual([]);
    applyViewUpdates();
    expect(out.pushes).toEqual([
      {type: 'add', node: {row: {id: 'e1', name: 'name1'}, relationships: {}}},
      {type: 'add', node: {row: {id: 'e2', name: 'name2'}, relationships: {}}},
      {
        type: 'edit',
        row: {id: 'e1', name: 'name1.1'},
        oldRow: {id: 'e1', name: 'name1'},
      },
    ]);
  };
  const context = new ZeroContext(
    new LogContext('info'),
    new IVMSourceBranch(schema.tables),
    null as unknown as AddQuery,
    null as unknown as AddCustomQuery,
    null as unknown as UpdateQuery,
    null as unknown as UpdateCustomQuery,
    null as unknown as FlushQueryChanges,
    batchViewUpdates,
    () => {},
    assertValidRunOptions,
  );
  const out = new Catch(
    // oxlint-disable-next-line no-non-null-assertion
    context.getSource('t1')!.connect([
      ['name', 'desc'],
      ['id', 'desc'],
    ]),
  );

  expect(batchViewUpdatesCalls).toBe(0);
  context.processChanges(undefined, 'ahash' as Hash, [
    {
      key: `${ENTITIES_KEY_PREFIX}t1/e1`,
      op: 'add',
      newValue: {id: 'e1', name: 'name1'},
    },
    {
      key: `${ENTITIES_KEY_PREFIX}t1/e2`,
      op: 'add',
      newValue: {id: 'e2', name: 'name2'},
    },
    {
      key: `${ENTITIES_KEY_PREFIX}t1/e1`,
      op: 'change',
      oldValue: {id: 'e1', name: 'name1'},
      newValue: {id: 'e1', name: 'name1.1'},
    },
  ]);
  expect(batchViewUpdatesCalls).toBe(1);
});

test('transactions', () => {
  const schema = createSchema({
    tables: [
      table('server')
        .columns({
          id: string(),
        })
        .primaryKey('id'),
      table('flair')
        .columns({
          id: string(),
          serverID: string(),
          description: string(),
        })
        .primaryKey('id'),
    ],
  });

  const context = new ZeroContext(
    new LogContext('info'),
    new IVMSourceBranch(schema.tables),
    null as unknown as AddQuery,
    null as unknown as AddCustomQuery,
    null as unknown as UpdateQuery,
    null as unknown as UpdateCustomQuery,
    null as unknown as FlushQueryChanges,
    testBatchViewUpdates,
    () => {},
    assertValidRunOptions,
  );
  const servers = context.getSource('server')!;
  const flair = context.getSource('flair')!;
  const join = new Join({
    parent: servers.connect([['id', 'asc']]),
    child: flair.connect([['id', 'asc']]),
    parentKey: ['id'],
    childKey: ['serverID'],
    hidden: false,
    relationshipName: 'flair',
    system: 'client',
  });
  const out = new Catch(join);

  const changes: NoIndexDiff = [
    {
      key: `${ENTITIES_KEY_PREFIX}server/s1`,
      op: 'add',
      newValue: {id: 's1', name: 'joanna'},
    },
    {
      key: `${ENTITIES_KEY_PREFIX}server/s2`,
      op: 'add',
      newValue: {id: 's2', name: 'brian'},
    },
    ...Array.from({length: 15})
      .fill(0)
      .map((_, i) => ({
        key: `${ENTITIES_KEY_PREFIX}flair/f${i}`,
        op: 'add' as const,
        newValue: {id: `f${i}`, serverID: 's1', description: `desc${i}`},
      })),
    ...Array.from({length: 37})
      .fill(0)
      .map((_, i) => ({
        key: `${ENTITIES_KEY_PREFIX}flair/f${15 + i}`,
        op: 'add' as const,
        newValue: {
          id: `f${15 + i}`,
          serverID: 's2',
          description: `desc${15 + i}`,
        },
      })),
  ];

  let transactions = 0;

  const remove = context.onTransactionCommit(() => {
    ++transactions;
  });
  remove();

  context.onTransactionCommit(() => {
    ++transactions;
  });

  context.processChanges(undefined, 'ahash' as Hash, changes);

  expect(transactions).toEqual(1);
  const result = out.fetch({}).filter(n => n !== 'yield');
  expect(result).length(2);
  expect(result[0].row).toEqual({id: 's1', name: 'joanna'});
  expect(result[0].relationships.flair).length(15);
  expect(result[1].row).toEqual({id: 's2', name: 'brian'});
  expect(result[1].relationships.flair).length(37);
});

test('batchViewUpdates errors if applyViewUpdates is not called', () => {
  let batchViewUpdatesCalls = 0;
  const batchViewUpdates = (_applyViewUpdates: () => void) => {
    batchViewUpdatesCalls++;
  };
  const context = new ZeroContext(
    new LogContext('info'),
    new IVMSourceBranch(schema.tables),
    null as unknown as AddQuery,
    null as unknown as AddCustomQuery,
    null as unknown as UpdateQuery,
    null as unknown as UpdateCustomQuery,
    null as unknown as FlushQueryChanges,
    batchViewUpdates,
    () => {},
    assertValidRunOptions,
  );

  expect(batchViewUpdatesCalls).toEqual(0);
  expect(() => context.batchViewUpdates(() => {})).toThrowError();
  expect(batchViewUpdatesCalls).toEqual(1);
});

test('batchViewUpdates returns value', () => {
  let batchViewUpdatesCalls = 0;
  const batchViewUpdates = (applyViewUpdates: () => void) => {
    applyViewUpdates();
    batchViewUpdatesCalls++;
  };
  const context = new ZeroContext(
    new LogContext('info'),
    new IVMSourceBranch(schema.tables),

    null as unknown as AddQuery,
    null as unknown as AddCustomQuery,
    null as unknown as UpdateQuery,
    null as unknown as UpdateCustomQuery,
    null as unknown as FlushQueryChanges,
    batchViewUpdates,
    () => {},
    assertValidRunOptions,
  );

  expect(batchViewUpdatesCalls).toEqual(0);
  expect(context.batchViewUpdates(() => 'test value')).toEqual('test value');
  expect(batchViewUpdatesCalls).toEqual(1);
});

test('synced top-level aggregate (count) reads the server value, not a local count', () => {
  // The client never syncs the underlying rows for a top-level aggregate; the
  // server consumes them and streams the precomputed result to a synthetic
  // table `aggregate:<queryID>`. The client reads from there (aggregatesFromSource)
  // rather than counting locally — so the value reflects the SERVER total even
  // though the client holds zero `t1` rows.
  const context = new ZeroContext(
    new LogContext('info'),
    new IVMSourceBranch(schema.tables),
    null as unknown as AddQuery,
    null as unknown as AddCustomQuery,
    null as unknown as UpdateQuery,
    null as unknown as UpdateCustomQuery,
    null as unknown as FlushQueryChanges,
    testBatchViewUpdates,
    () => {},
    assertValidRunOptions,
  );

  const queryID = 'q1';
  const aggTable = topLevelAggregateTableName(queryID);
  expect(aggTable).toBe('aggregate:q1');

  // `t1.count()` — buildPipeline takes the aggregatesFromSource short-circuit
  // (ZeroContext sets it) and reads the synthetic source; no `t1` source needed.
  const ast: AST = {table: 't1', aggregate: {fn: 'count'}};
  const view = new ArrayView(
    buildPipeline(ast, context, queryID),
    {...defaultFormat, singular: true, aggregate: {fn: 'count'}},
    true,
    () => {},
  );

  // No aggregate row synced yet → no value.
  expect(view.data).toBeUndefined();

  // Server streams the count (it consumed the t1 rows; they never sync). The
  // synthetic row carries the singleton key, the value, and a stamped version.
  context.processChanges(undefined, 'h1' as Hash, [
    {
      key: `${ENTITIES_KEY_PREFIX}${aggTable}/0`,
      op: 'add',
      newValue: {'': 0, 'value': 42, ['_0_version']: '01'},
    },
  ]);
  expect(view.data).toBe(42);

  // Server recomputes (e.g. a row was inserted upstream): 42 -> 43.
  context.processChanges('h1' as Hash, 'h2' as Hash, [
    {
      key: `${ENTITIES_KEY_PREFIX}${aggTable}/0`,
      op: 'change',
      oldValue: {'': 0, 'value': 42, ['_0_version']: '01'},
      newValue: {'': 0, 'value': 43, ['_0_version']: '02'},
    },
  ]);
  expect(view.data).toBe(43);

  // The client holds zero `t1` rows the whole time — the count is purely the
  // synced server value.
  const t1 = context.getSource('t1');
  assert(t1, 'Expected t1 source');
  expect([...t1.connect([['id', 'asc']]).fetch({})]).toEqual([]);
});

test('getSource provisions a synthetic source for a top-level aggregate table', () => {
  const context = new ZeroContext(
    new LogContext('info'),
    new IVMSourceBranch(schema.tables),
    null as unknown as AddQuery,
    null as unknown as AddCustomQuery,
    null as unknown as UpdateQuery,
    null as unknown as UpdateCustomQuery,
    null as unknown as FlushQueryChanges,
    testBatchViewUpdates,
    () => {},
    assertValidRunOptions,
  );

  const source = context.getSource('aggregate:abc');
  assert(source instanceof MemorySource, 'Expected a MemorySource');
  expect(source.tableSchema.primaryKey).toEqual(['']);
  expect(Object.keys(source.tableSchema.columns).sort()).toEqual(['', 'value']);
  // Cached.
  expect(context.getSource('aggregate:abc')).toBe(source);

  // Relationship aggregate tables are NOT auto-provisioned (need the
  // correlation key); they remain unknown for now.
  expect(context.getSource('aggregate:abc:comments')).toBeUndefined();
});

test('synced relationship aggregate (count) reads the server per-parent value', () => {
  // issue.related('comments', c => c.count()): the server streams one synthetic
  // row per issue to aggregate:<queryID>:<alias>; the client reads them and
  // never holds the comment rows.
  const relSchema = createSchema({
    tables: [
      table('issue').columns({id: string(), title: string()}).primaryKey('id'),
      table('comment')
        .columns({id: string(), issueID: string()})
        .primaryKey('id'),
    ],
  });
  const context = new ZeroContext(
    new LogContext('info'),
    new IVMSourceBranch(relSchema.tables),
    null as unknown as AddQuery,
    null as unknown as AddCustomQuery,
    null as unknown as UpdateQuery,
    null as unknown as UpdateCustomQuery,
    null as unknown as FlushQueryChanges,
    testBatchViewUpdates,
    () => {},
    assertValidRunOptions,
  );

  const queryID = 'rq';
  const relatedSq: CorrelatedSubquery = {
    system: 'client',
    correlation: {parentField: ['id'], childField: ['issueID']},
    aggregate: {fn: 'count'},
    subquery: {
      table: 'comment',
      alias: 'commentCount',
      orderBy: [['id', 'asc']],
    },
  };
  const ast: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
    related: [relatedSq],
  };
  const aggTable = aggregateTableName(queryID, relatedSq);
  expect(aggTable).toBe('aggregate:rq:commentCount');

  const view = new ArrayView(
    buildPipeline(ast, context, queryID),
    {
      ...defaultFormat,
      relationships: {
        commentCount: {
          ...defaultFormat,
          singular: true,
          aggregate: {fn: 'count'},
        },
      },
    },
    true,
    () => {},
  );

  // Parent issues sync as usual.
  context.processChanges(undefined, 'h1' as Hash, [
    {
      key: `${ENTITIES_KEY_PREFIX}issue/1`,
      op: 'add',
      newValue: {id: '1', title: 'a'},
    },
    {
      key: `${ENTITIES_KEY_PREFIX}issue/2`,
      op: 'add',
      newValue: {id: '2', title: 'b'},
    },
  ]);
  // Per-issue counts stream to the synthetic table (issue 1 -> 3, issue 2 -> 0).
  context.processChanges('h1' as Hash, 'h2' as Hash, [
    {
      key: `${ENTITIES_KEY_PREFIX}${aggTable}/1`,
      op: 'add',
      newValue: {issueID: '1', value: 3, ['_0_version']: '01'},
    },
    {
      key: `${ENTITIES_KEY_PREFIX}${aggTable}/2`,
      op: 'add',
      newValue: {issueID: '2', value: 0, ['_0_version']: '01'},
    },
  ]);

  // Strip the view's internal refcount Symbol(rc) added to list entries.
  const clean = (data: unknown) =>
    (data as ReadonlyArray<Record<string, unknown>>).map(r => ({
      id: r.id,
      title: r.title,
      commentCount: r.commentCount,
    }));
  expect(clean(view.data)).toEqual([
    {id: '1', title: 'a', commentCount: 3},
    {id: '2', title: 'b', commentCount: 0},
  ]);

  // No comment rows on the client.
  const comments = context.getSource('comment');
  assert(comments, 'Expected comment source');
  expect([...comments.connect([['id', 'asc']]).fetch({})]).toEqual([]);

  // Server recomputes issue 1: 3 -> 4.
  context.processChanges('h2' as Hash, 'h3' as Hash, [
    {
      key: `${ENTITIES_KEY_PREFIX}${aggTable}/1`,
      op: 'change',
      oldValue: {issueID: '1', value: 3, ['_0_version']: '01'},
      newValue: {issueID: '1', value: 4, ['_0_version']: '02'},
    },
  ]);
  expect(clean(view.data)).toEqual([
    {id: '1', title: 'a', commentCount: 4},
    {id: '2', title: 'b', commentCount: 0},
  ]);
});

test('getOrCreateAggregateSource provisions a relationship aggregate source', () => {
  const branch = new IVMSourceBranch(schema.tables);
  const source = branch.getOrCreateAggregateSource(
    'aggregate:q:commentCount',
    {issueID: {type: 'string'}, value: {type: 'number'}},
    ['issueID'],
  );
  expect(source.tableSchema.primaryKey).toEqual(['issueID']);
  // Idempotent.
  expect(
    branch.getOrCreateAggregateSource(
      'aggregate:q:commentCount',
      {issueID: {type: 'string'}, value: {type: 'number'}},
      ['issueID'],
    ),
  ).toBe(source);
});
