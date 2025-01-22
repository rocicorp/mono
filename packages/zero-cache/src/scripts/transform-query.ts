/* eslint-disable no-console */
import 'dotenv/config';

import {getDebugConfig} from '../config/zero-config.js';
import {getSchema} from '../auth/load-schema.js';
import {transformAndHashQuery} from '../auth/read-authorizer.js';
import {must} from '../../../shared/src/must.js';
import {pgClient} from '../types/pg.js';
import {consoleLogSink, LogContext} from '@rocicorp/logger';

const config = getDebugConfig();
const schema = await getSchema(config);

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
