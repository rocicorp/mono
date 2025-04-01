/* eslint-disable no-console */
import '@dotenvx/dotenvx/config';

import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {must} from '../../../shared/src/must.ts';
import {parseOptions} from '../../../shared/src/options.ts';
import * as v from '../../../shared/src/valita.ts';
import {transformAndHashQuery} from '../auth/read-authorizer.ts';
import {
  appOptions,
  shardOptions,
  ZERO_ENV_VAR_PREFIX,
} from '../config/zero-config.ts';
import {pgClient} from '../types/pg.ts';
import {
  deployPermissionsOptions,
  loadSchemaAndPermissions,
} from './permissions.ts';
import {getShardID, upstreamSchema} from '../types/shards.ts';
import {astToZQL} from '../../../ast-to-zql/src/ast-to-zql.ts';
import {formatOutput} from '../../../ast-to-zql/src/format.ts';

const options = {
  cvr: {db: v.string()},
  schema: deployPermissionsOptions.schema,
  app: appOptions,
  shard: shardOptions,
  debug: {
    hash: {
      type: v.string().optional(),
      desc: ['Hash of the query to fetch the AST for.'],
    },
  },
};

const config = parseOptions(
  options,
  process.argv.slice(2),
  ZERO_ENV_VAR_PREFIX,
);

const lc = new LogContext('debug', {}, consoleLogSink);
const {permissions} = await loadSchemaAndPermissions(lc, config.schema.path);

const cvrDB = pgClient(lc, config.cvr.db);

const rows =
  await cvrDB`select "clientAST", "internal" from ${cvrDB(upstreamSchema(getShardID(config)) + '/cvr')}."queries" where "queryHash" = ${must(
    config.debug.hash,
  )} limit 1;`;

const queryAst = transformAndHashQuery(
  lc,
  rows[0].clientAST,
  permissions,
  {},
  rows[0].internal,
).query;

console.log('\n=== AST ===\n');
console.log(JSON.stringify(queryAst, null, 2));
console.log('\n=== ZQL ===\n');
console.log(await formatOutput(queryAst.table + astToZQL(queryAst)));

await cvrDB.end();
