import {must} from '../../../shared/src/must.js';
import {Replicator} from '../replicator/replicator.js';
import {consoleLogSink, LogContext} from '@rocicorp/logger';

const pgConnectionString = process.env.PG_CONNECTION_STRING;
const sqliteDbPath = process.env.SQLITE_DB_PATH;

const lc = new LogContext('info', undefined, consoleLogSink).withContext(
  'component',
  'Replicator',
);
await new Replicator(must(pgConnectionString), must(sqliteDbPath)).start(lc);
