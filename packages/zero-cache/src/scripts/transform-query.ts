/* eslint-disable no-console */
import 'dotenv/config';

import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {must} from '../../../shared/src/must.ts';
import {getPermissions} from '../auth/load-schema.ts';
import {transformAndHashQuery} from '../auth/read-authorizer.ts';
import {getDebugConfig} from '../config/zero-config.ts';
import {pgClient} from '../types/pg.ts';

const config = getDebugConfig();
const schema = await getPermissions(config);

const cvrDB = pgClient(
  new LogContext('debug', undefined, consoleLogSink),
  config.cvr.db,
);

const rows =
  await cvrDB`select "clientAST" from "cvr"."queries" where "queryHash" = ${must(
    config.debug.hash,
  )} limit 1;`;

console.log(
  JSON.stringify(
    transformAndHashQuery(rows[0].clientAST, schema.permissions, {}).query,
  ),
);

await cvrDB.end();
