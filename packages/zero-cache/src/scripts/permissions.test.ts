import {dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {describe, expect, test} from 'vitest';
import {loadSchemaAndPermissions} from './permissions.ts';

describe('scripts/permissions', () => {
  test('loads schema with permissions', async () => {
    const schemaPath = join(
      dirname(fileURLToPath(import.meta.url)),
      'schema-permissions-test.ts',
    );

    const result = await loadSchemaAndPermissions(schemaPath);

    expect(result?.schema?.tables?.member).toBeDefined();
    expect(result.permissions?.tables?.['member']?.row?.insert?.[0])
      .toMatchInlineSnapshot(`
      [
        "allow",
        {
          "conditions": [],
          "type": "and",
        },
      ]
    `);
  });

  test('loads schema without permissions', async () => {
    const schemaPath = join(
      dirname(fileURLToPath(import.meta.url)),
      'schema-no-permissions-test.ts',
    );

    const result = await loadSchemaAndPermissions(schemaPath);

    expect(result?.schema?.tables?.member).toBeDefined();
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
