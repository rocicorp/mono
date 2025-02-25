import {beforeEach, describe, expect, test} from 'vitest';
import {h128} from '../../../shared/src/hash.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
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
      CREATE TABLE "zero_app.permissions" (
        permissions JSON,
        hash TEXT
      );
      INSERT INTO "zero_app.permissions" (permissions) VALUES (NULL);
      `);
    db = new StatementRunner(replica);
  });

  function setPermissions(perms: PermissionsConfig | string) {
    const permissions =
      typeof perms === 'string' ? perms : JSON.stringify(perms);
    replica
      .prepare(`UPDATE "zero_app.permissions" SET permissions = ?, hash = ?`)
      .run(permissions, h128(permissions).toString(16));
  }

  test('loads supported permissions', () => {
    setPermissions({tables: {}});
    expect(loadPermissions(lc, db, 'zero_app')).toMatchInlineSnapshot(`
      {
        "hash": "4fa6194de2f465d532971ce1b9b513e9",
        "permissions": {
          "tables": {},
        },
      }
    `);
  });

  test('invalid permissions', () => {
    setPermissions(`{"tablez":{}}`);
    expect(() =>
      loadPermissions(lc, db, 'zero_app'),
    ).toThrowErrorMatchingInlineSnapshot(
      `
      [Error: Could not parse upstream permissions: '{"tablez":{}}'.
      This may happen if Permissions with a new internal format are deployed before the supporting server has been fully rolled out.]
    `,
    );
  });

  test('permissions invalid JSON', () => {
    setPermissions(`I'm not JSON`);
    expect(() =>
      loadPermissions(lc, db, 'zero_app'),
    ).toThrowErrorMatchingInlineSnapshot(
      `
      [Error: Could not parse upstream permissions: 'I'm not JSON'.
      This may happen if Permissions with a new internal format are deployed before the supporting server has been fully rolled out.]
    `,
    );
  });

  test('invalid long permissions', () => {
    setPermissions(`{"baz": 108, "foo":"ba${'a'.repeat(1000)}r"}`);
    expect(() =>
      loadPermissions(lc, db, 'zero_app'),
    ).toThrowErrorMatchingInlineSnapshot(
      `
      [Error: Could not parse upstream permissions: '{"baz": 108, "foo":"baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa...'.
      This may happen if Permissions with a new internal format are deployed before the supporting server has been fully rolled out.]
    `,
    );
  });
});
