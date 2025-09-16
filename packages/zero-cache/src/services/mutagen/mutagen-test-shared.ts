/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import {ident as id} from 'pg-format';
import {appSchema, upstreamSchema, type ShardID} from '../../types/shards.ts';

export function zeroSchema(shardID: ShardID): string {
  const shard = id(upstreamSchema(shardID));
  const app = id(appSchema(shardID));
  return /*sql*/ `
      CREATE SCHEMA ${shard};
      CREATE TABLE ${shard}.clients (
        "clientGroupID"  TEXT NOT NULL,
        "clientID"       TEXT NOT NULL,
        "lastMutationID" BIGINT,
        "userID"         TEXT,
        PRIMARY KEY ("clientGroupID", "clientID")
      );
      CREATE SCHEMA ${app};
      CREATE TABLE ${app}."schemaVersions" (
        "minSupportedVersion" INT4,
        "maxSupportedVersion" INT4,

        -- Ensure that there is only a single row in the table.
        -- Application code can be agnostic to this column, and
        -- simply invoke UPDATE statements on the version columns.
        "lock" BOOL PRIMARY KEY DEFAULT true CHECK (lock)
      );
      INSERT INTO ${app}."schemaVersions" ("lock", "minSupportedVersion", "maxSupportedVersion")
        VALUES (true, 1, 1);`;
}
