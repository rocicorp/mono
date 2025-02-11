import type {LogContext} from '@rocicorp/logger';
import * as v from '../../../shared/src/valita.ts';
import {
  MIN_SERVER_SUPPORTED_PERMISSIONS_PROTOCOL,
  PROTOCOL_VERSION,
} from '../../../zero-protocol/src/protocol-version.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  permissionsConfigSchema,
  type PermissionsConfig,
} from '../../../zero-schema/src/compiled-permissions.ts';
import type {TableSchema} from '../../../zero-schema/src/table-schema.ts';
import type {Database} from '../../../zqlite/src/db.ts';
import {computeZqlSpecs} from '../db/lite-tables.ts';
import type {StatementRunner} from '../db/statements.ts';

export type LoadedPermissions = {
  permissions: PermissionsConfig | null;
  hash: string | null;
};

const errorPreamble =
  `This server supports Permissions protocol versions v` +
  `${MIN_SERVER_SUPPORTED_PERMISSIONS_PROTOCOL} through v${PROTOCOL_VERSION}`;

export function loadPermissions(
  lc: LogContext,
  replica: StatementRunner,
): LoadedPermissions {
  const {permissions, hash} = replica.get(
    `SELECT permissions, hash FROM "zero.permissions"`,
  );
  if (permissions === null) {
    lc.warn?.(
      `\n\n\n` +
        `No upstream permissions deployed.\n` +
        `Run 'npx zero-deploy-permissions' to enforce permissions.` +
        `\n\n\n`,
    );
    return {permissions, hash: null};
  }
  let obj;
  let parsed;
  try {
    obj = JSON.parse(permissions);
    parsed = v.parse(obj, permissionsConfigSchema);
  } catch (e) {
    throw new Error(
      `${errorPreamble}.\nCould not parse upstream permissions${
        obj ? ` at v${obj.protocolVersion}` : `: ${permissions}`
      }`,
      {cause: e},
    );
  }
  if (parsed.protocolVersion > PROTOCOL_VERSION) {
    throw new Error(
      `${errorPreamble} and cannot read ` +
        `v${parsed.protocolVersion}.\nPlease deploy the latest server.`,
    );
  }
  if (parsed.protocolVersion < MIN_SERVER_SUPPORTED_PERMISSIONS_PROTOCOL) {
    throw new Error(
      `${errorPreamble} and no longer supports ` +
        `v${parsed.protocolVersion}.\nRun 'npx zero-deploy-permissions' ` +
        `to deploy Permissions in the latest format.`,
    );
  }
  lc.debug?.(`Loaded permissions (hash: ${hash})`);
  return {permissions: parsed, hash};
}

export function reloadPermissionsIfChanged(
  lc: LogContext,
  replica: StatementRunner,
  current: LoadedPermissions | null,
): {permissions: LoadedPermissions; changed: boolean} {
  if (current === null) {
    return {permissions: loadPermissions(lc, replica), changed: true};
  }
  const {hash} = replica.get(`SELECT hash FROM "zero.permissions"`);
  return hash === current.hash
    ? {permissions: current, changed: false}
    : {permissions: loadPermissions(lc, replica), changed: true};
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
