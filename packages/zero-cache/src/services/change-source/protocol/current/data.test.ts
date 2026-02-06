import {expect, expectTypeOf, test} from 'vitest';
import {
  isSchemaChange,
  type Change,
  type SchemaChange,
  type SchemaChangeTag,
} from './data.ts';

test('schema change tag', () => {
  expectTypeOf<SchemaChangeTag>().toEqualTypeOf<SchemaChange['tag']>;

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
  }

  for (const tag of [
    'insert',
    'update',
    'backfill',
    'truncate',
    'delete',
    'begin',
    'commit',
    'rollback',
    'status',
  ]) {
    expect(isSchemaChange({tag} as Change)).toBe(false);
  }
});
