import '../../shared/src/dotenv.ts';

import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {astToZQL} from '../../ast-to-zql/src/ast-to-zql.ts';
import {formatOutput} from '../../ast-to-zql/src/format.ts';
import {must} from '../../shared/src/must.ts';
import {parseOptions} from '../../shared/src/options.ts';
import * as v from '../../shared/src/valita.ts';
import {transformAndHashQuery} from '../../zero-cache/src/auth/read-authorizer.ts';
import {
  appOptions,
  shardOptions,
  ZERO_ENV_VAR_PREFIX,
} from '../../zero-cache/src/config/zero-config.ts';
import {loadSchemaAndPermissions} from '../../zero-cache/src/scripts/permissions.ts';
import {pgClient} from '../../zero-cache/src/types/pg.ts';
import {getShardID, upstreamSchema} from '../../zero-cache/src/types/shards.ts';

const options = {
  cvr: {db: v.string()},
  schema: {
    type: v.string().default('./schema.ts'),
    desc: ['Path to the schema file.'],
  },
  app: appOptions,
  shard: shardOptions,
  hash: {
    type: v.string().optional(),
    desc: ['Hash of the query to fetch the AST for.'],
  },
};

const config = parseOptions(options, {envNamePrefix: ZERO_ENV_VAR_PREFIX});

const lc = new LogContext('debug', {}, consoleLogSink);
const {permissions} = await loadSchemaAndPermissions(config.schema);

const cvrDB = pgClient(lc, config.cvr.db);

const rows =
  await cvrDB`select "clientAST", "internal" from ${cvrDB(upstreamSchema(getShardID(config)) + '/cvr')}."queries" where "queryHash" = ${must(
    config.hash,
  )} limit 1;`;

const queryAst = transformAndHashQuery(
  lc,
  '',
  rows[0].clientAST,
  permissions,
  {
    type: 'jwt',
    raw: '',
    decoded: {},
  },
  rows[0].internal,
).transformedAst;

// oxlint-disable no-console
console.log('\n=== AST ===\n');
console.log(JSON.stringify(queryAst, null, 2));
console.log('\n=== ZQL ===\n');
console.log(await formatOutput(queryAst.table + astToZQL(queryAst)));
// oxlint-enable no-console

await cvrDB.end();
