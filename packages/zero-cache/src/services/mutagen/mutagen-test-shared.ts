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
