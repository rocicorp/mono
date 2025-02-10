import type {LogContext} from '@rocicorp/logger';
import {readFile} from 'node:fs/promises';
import path from 'node:path';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {type PermissionsConfig} from '../../../zero-schema/src/compiled-permissions.ts';
import {parseSchema} from '../../../zero-schema/src/schema-config.ts';
import type {TableSchema} from '../../../zero-schema/src/table-schema.ts';
import type {Database} from '../../../zqlite/src/db.ts';
import type {ZeroConfig} from '../config/zero-config.ts';
import {computeZqlSpecs} from '../db/lite-tables.ts';

let loadedPermissions: Promise<{permissions: PermissionsConfig}> | undefined;

export function getPermissions(
  config: ZeroConfig,
): Promise<{permissions: PermissionsConfig}> {
  if (loadedPermissions) {
    return loadedPermissions;
  }

  loadedPermissions = (async () => {
    if (config.schema.json) {
      return parseSchema(config.schema.json, 'config.schema.json');
    }
    const fileContent = await readFile(
      path.resolve(config.schema.file),
      'utf-8',
    );
    return parseSchema(fileContent, config.schema.file);
  })();

  return loadedPermissions;
}

export function getSchema(lc: LogContext, replica: Database): Schema {
  const specs = computeZqlSpecs(lc, replica);
  const tables = Object.fromEntries(
    [...specs.values()].map(table => {
      const {
        tableSpec: {name, primaryKey},
        zqlSpec: columns,
      } = table;
      return [name, {name, columns, primaryKey} satisfies TableSchema];
    }),
  );
  return {
    version: 1, // only used on the client-side
    tables,
    relationships: {}, // relationships are already denormalized in ASTs
  };
}
