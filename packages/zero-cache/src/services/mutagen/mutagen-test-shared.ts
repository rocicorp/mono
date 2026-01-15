import {ident as id} from 'pg-format';
import {upstreamSchema, type ShardID} from '../../types/shards.ts';

export function zeroSchema(shardID: ShardID): string {
  const shard = id(upstreamSchema(shardID));
  return /*sql*/ `
      CREATE SCHEMA ${shard};
      CREATE TABLE ${shard}.clients (
        "clientGroupID"  TEXT NOT NULL,
        "clientID"       TEXT NOT NULL,
        "lastMutationID" BIGINT,
        "userID"         TEXT,
        PRIMARY KEY ("clientGroupID", "clientID")
      );`;
}
