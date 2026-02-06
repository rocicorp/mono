import {expect, expectTypeOf, test} from 'vitest';
import {
  isDataChange,
  isSchemaChange,
  type Change,
  type DataChange,
  type DataChangeTag,
  type SchemaChange,
  type SchemaChangeTag,
} from './data.ts';

test('schema and data change tags', () => {
  expectTypeOf<SchemaChangeTag>().toEqualTypeOf<SchemaChange['tag']>;
  expectTypeOf<DataChangeTag>().toEqualTypeOf<DataChange['tag']>;

  // Sanity check. The type check above should cover everything.
  for (const tag of [
    'create-table',
    'rename-table',
    'update-table-metadata',
    'add-column',
    'update-column',
    'drop-column',
    'drop-table',
    'create-index',
    'drop-index',
    'backfill-completed',
  ]) {
    expect(isSchemaChange({tag} as Change)).toBe(true);
    expect(isDataChange({tag} as Change)).toBe(false);
  }

  for (const tag of ['insert', 'update', 'backfill', 'truncate', 'delete']) {
    expect(isSchemaChange({tag} as Change)).toBe(false);
    expect(isDataChange({tag} as Change)).toBe(true);
  }

  for (const tag of ['begin', 'commit', 'status', 'rollback']) {
    expect(isSchemaChange({tag} as Change)).toBe(false);
    expect(isDataChange({tag} as Change)).toBe(false);
  }
});
