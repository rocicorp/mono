import {beforeEach, describe, expect, test} from 'vitest';
import {h128} from '../../../shared/src/hash.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {PROTOCOL_VERSION} from '../../../zero-protocol/src/protocol-version.ts';
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

  test('invalid permissions', () => {
    setPermissions(`{"protocolVersion": 108}`);
    expect(() => loadPermissions(lc, db)).toThrowErrorMatchingInlineSnapshot(
      `
      [Error: Could not parse upstream permissions: '{"protocolVersion": 108}...'.
      This may happen if Permissions with a new internal format are deployed before the supporting server has been fully rolled out.]
    `,
    );
  });

  test('permissions invalid JSON', () => {
    setPermissions(`I'm not JSON`);
    expect(() => loadPermissions(lc, db)).toThrowErrorMatchingInlineSnapshot(
      `
      [Error: Could not parse upstream permissions: 'I'm not JSON...'.
      This may happen if Permissions with a new internal format are deployed before the supporting server has been fully rolled out.]
    `,
    );
  });
});
