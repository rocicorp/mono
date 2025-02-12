import {beforeEach, describe, expect, test} from 'vitest';
import {h128} from '../../../shared/src/hash.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {
  MIN_SERVER_SUPPORTED_PERMISSIONS_PROTOCOL,
  PROTOCOL_VERSION,
} from '../../../zero-protocol/src/protocol-version.ts';
import type {PermissionsConfig} from '../../../zero-schema/src/compiled-permissions.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {StatementRunner} from '../db/statements.ts';
import {loadPermissions} from './load-permissions.ts';

describe('auth/load-permissions', () => {
  const lc = createSilentLogContext();
  let replica: Database;
  let db: StatementRunner;

  beforeEach(() => {
    replica = new Database(createSilentLogContext(), ':memory:');
    replica.exec(/* sql */ `
      CREATE TABLE "zero.permissions" (
        permissions JSON,
        hash TEXT
      );
      INSERT INTO "zero.permissions" (permissions) VALUES (NULL);
      `);
    db = new StatementRunner(replica);
  });

  function setPermissions(perms: PermissionsConfig | string) {
    const permissions =
      typeof perms === 'string' ? perms : JSON.stringify(perms);
    replica
      .prepare(`UPDATE "zero.permissions" SET permissions = ?, hash = ?`)
      .run(permissions, h128(permissions).toString(16));
  }

  test('loads supported permissions', () => {
    setPermissions({
      protocolVersion: PROTOCOL_VERSION,
      tables: {},
    });
    expect(loadPermissions(lc, db)).toMatchInlineSnapshot(`
      {
        "hash": "5798cf58470da01180f25d9ad4bdd92d",
        "permissions": {
          "protocolVersion": 5,
          "tables": {},
        },
      }
    `);
  });

  test('permissions version ahead', () => {
    setPermissions({
      protocolVersion: PROTOCOL_VERSION + 1,
      tables: {},
    });
    expect(() => loadPermissions(lc, db)).toThrowErrorMatchingInlineSnapshot(
      `
      [Error: This server supports Permissions protocol versions v4 through v5 and cannot read v6.
      Please deploy the latest server.]
    `,
    );
  });

  test('permissions version behind', () => {
    setPermissions({
      protocolVersion: MIN_SERVER_SUPPORTED_PERMISSIONS_PROTOCOL - 1,
      tables: {},
    });
    expect(() => loadPermissions(lc, db)).toThrowErrorMatchingInlineSnapshot(
      `
      [Error: This server supports Permissions protocol versions v4 through v5 and no longer supports v3.
      Run 'npx zero-deploy-permissions' to deploy Permissions in the latest format.]
    `,
    );
  });

  test('invalid permissions', () => {
    setPermissions(`{"protocolVersion": 108}`);
    expect(() => loadPermissions(lc, db)).toThrowErrorMatchingInlineSnapshot(
      `
      [Error: This server supports Permissions protocol versions v4 through v5.
      Could not parse upstream permissions at v108]
    `,
    );
  });

  test('permissions invalid JSON', () => {
    setPermissions(`I'm not JSON`);
    expect(() => loadPermissions(lc, db)).toThrowErrorMatchingInlineSnapshot(
      `
      [Error: This server supports Permissions protocol versions v4 through v5.
      Could not parse upstream permissions: I'm not JSON]
    `,
    );
  });
});
