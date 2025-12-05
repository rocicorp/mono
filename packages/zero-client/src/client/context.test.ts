import {LogContext} from '@rocicorp/logger';
import {expect, test} from 'vitest';
import type {NoIndexDiff} from '../../../replicache/src/btree/node.ts';
import type {Hash} from '../../../replicache/src/hash.ts';
import {assert} from '../../../shared/src/asserts.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../zero-schema/src/builder/table-builder.ts';
import {Catch} from '../../../zql/src/ivm/catch.ts';
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
  assert(source instanceof MemorySource);
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
      oldRow: {id: 'e1', name: 'name1'},
      row: {id: 'e1', name: 'name1.1'},
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
        oldRow: {id: 'e1', name: 'name1'},
        row: {id: 'e1', name: 'name1.1'},
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
