import {join, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {describe, expect, test} from 'vitest';
import {loadSchemaAndPermissions} from './permissions.ts';

describe('scripts/permissions', () => {
  test('loads schema without permissions', async () => {
    // Use the zbugs schema which doesn't have permissions
    const repoRoot = resolve(fileURLToPath(import.meta.url), '../../../../../');
    const schemaPath = join(repoRoot, 'apps/zbugs/shared/schema.ts');

    const result = await loadSchemaAndPermissions(schemaPath);

    expect(result).toBeDefined();
    expect(result.schema).toBeDefined();
    expect(result.schema.tables).toBeDefined();
    expect(result.schema.tables.user).toBeDefined();
    expect(result.schema.tables.issue).toBeDefined();
    // When permissions are not defined, it defaults to an empty object
    expect(result.permissions).toEqual({});
  });

  test('handles missing schema file with allowMissing=true', async () => {
    const nonExistentPath = join(
      fileURLToPath(import.meta.url),
      '/does/not/exist.ts',
    );

    const result = await loadSchemaAndPermissions(nonExistentPath, true);

    expect(result).toBeUndefined();
  });
});
