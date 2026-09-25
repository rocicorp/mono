import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import type {IndexSpec} from '../../../db/specs.ts';
import {
  CREATE_INDEX_METADATA_TABLE,
  IndexMetadataStore,
} from './index-metadata.ts';

describe('IndexMetadataStore', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(createSilentLogContext(), ':memory:');
    db.exec(CREATE_INDEX_METADATA_TABLE);
  });

  test('setIndex and getIndex', () => {
    const store = IndexMetadataStore.getInstance(db)!;
    const spec: IndexSpec = {
      schema: 'public',
      tableName: 'issues',
      name: 'idx_created',
      columns: {created: 'ASC'},
      unique: false,
    };

    store.setIndex('issues', 'idx_created', spec);
    expect(store.getIndex('idx_created')).toEqual(spec);
    expect(store.getIndexesForTable('issues')).toEqual([
      {name: 'idx_created', spec},
    ]);
  });

  test('update existing index', () => {
    const store = IndexMetadataStore.getInstance(db)!;
    const spec1: IndexSpec = {
      schema: 'public',
      tableName: 'issues',
      name: 'idx_created',
      columns: {created: 'ASC'},
      unique: false,
    };
    const spec2: IndexSpec = {
      schema: 'public',
      tableName: 'issues',
      name: 'idx_created',
      columns: {created: 'DESC'},
      unique: false,
    };

    store.setIndex('issues', 'idx_created', spec1);
    store.setIndex('issues', 'idx_created', spec2);
    expect(store.getIndex('idx_created')).toEqual(spec2);
  });

  test('deleteIndex and deleteTable', () => {
    const store = IndexMetadataStore.getInstance(db)!;
    store.setIndex('issues', 'idx_1', {
      schema: 'public',
      tableName: 'issues',
      name: 'idx_1',
      columns: {a: 'ASC'},
      unique: false,
    });
    store.setIndex('issues', 'idx_2', {
      schema: 'public',
      tableName: 'issues',
      name: 'idx_2',
      columns: {b: 'ASC'},
      unique: false,
    });
    store.setIndex('users', 'idx_3', {
      schema: 'public',
      tableName: 'users',
      name: 'idx_3',
      columns: {c: 'ASC'},
      unique: false,
    });

    store.deleteIndex('idx_1');
    expect(store.getIndexesForTable('issues')).toEqual([
      {
        name: 'idx_2',
        spec: {
          schema: 'public',
          tableName: 'issues',
          name: 'idx_2',
          columns: {b: 'ASC'},
          unique: false,
        },
      },
    ]);

    store.deleteTable('issues');
    expect(store.getIndexesForTable('issues')).toEqual([]);
    expect(store.getIndexesForTable('users')).toHaveLength(1);
  });

  test('renameTable', () => {
    const store = IndexMetadataStore.getInstance(db)!;
    store.setIndex('issues', 'idx_1', {
      schema: 'public',
      tableName: 'issues',
      name: 'idx_1',
      columns: {a: 'ASC'},
      unique: false,
    });

    store.renameTable('issues', 'tasks');
    expect(store.getIndexesForTable('issues')).toEqual([]);
    expect(store.getIndexesForTable('tasks')).toHaveLength(1);
  });
});
