import {assert} from '../../../shared/src/asserts.ts';

export type AppID = {
  readonly appID: string;
};

export type ShardID = AppID & {
  readonly shardNum: number;
};

export type ShardConfig = ShardID & {
  readonly publications: readonly string[];
};

// Gets a ShardID from a ZeroConfig.
export function getShardID({
  app,
  shard,
}: {
  app: {id: string};
  shard: {num: number};
}): ShardID {
  return {
    appID: app.id,
    shardNum: shard.num,
  };
}

// Gets a ShardConfig from a ZeroConfig.
export function getShardConfig({
  app,
  shard,
}: {
  app: {id: string; publications: string[]};
  shard: {num: number};
}): ShardConfig {
  return {
    appID: app.id,
    shardNum: shard.num,
    publications: app.publications,
  };
}

// Constrained by https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS-MANIPULATION
export const ALLOWED_APP_ID_CHARACTERS = /^[a-z0-9_]+$/;

export const INVALID_APP_ID_MESSAGE =
  'The App ID may only consist of lower-case letters, numbers, and the underscore character';

export function check(shard: ShardID): {appID: string; shardNum: number} {
  const {appID, shardNum} = shard;
  if (!ALLOWED_APP_ID_CHARACTERS.test(appID)) {
    throw new Error(INVALID_APP_ID_MESSAGE);
  }
  assert(typeof shardNum === 'number', 'shardNum must be a number');
  return {appID, shardNum};
}

export function appSchema({appID}: AppID) {
  check({appID, shardNum: 0});
  return appID;
}

export function upstreamSchema(shard: ShardID) {
  const {appID, shardNum} = check(shard);
  return `${appID}_${shardNum}`;
}

export function cdcSchema(shard: ShardID) {
  const {appID, shardNum} = check(shard);
  return `${appID}_${shardNum}/cdc`;
}

export function cvrSchema(shard: ShardID) {
  const {appID, shardNum} = check(shard);
  return `${appID}_${shardNum}/cvr`;
}
