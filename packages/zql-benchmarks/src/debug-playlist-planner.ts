import {getChinook} from '../../zql-integration-tests/src/chinook/get-deps.ts';
import {schema} from '../../zql-integration-tests/src/chinook/schema.ts';
import {builder} from '../../zql-integration-tests/src/chinook/schema.ts';
import {createSQLiteCostModel} from '../../zqlite/src/sqlite-cost-model.ts';
import {
  clientToServer,
  serverToClient,
} from '../../zero-schema/src/name-mapper.ts';
import {planQuery} from '../../zql/src/planner/planner-builder.ts';
import {mapAST} from '../../zero-protocol/src/ast.ts';
import {Database} from '../../zqlite/src/db.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {initialSync} from '../../zero-cache/src/services/change-source/pg/initial-sync.ts';
import {testDBs, getConnectionURI} from '../../zero-cache/src/test/db.ts';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';

const pgContent = await getChinook();
const lc = createSilentLogContext();

// Create PostgreSQL database
const pg = await testDBs.create('debug_playlist_planner', undefined, false);
await pg.unsafe(pgContent);

// Create SQLite database
const tempDir = await fs.mkdtemp(
  path.join(os.tmpdir(), 'debug-playlist-planner'),
);
const tempFile = path.join(tempDir, 'debug.db');
const sqlite = new Database(lc, tempFile);
sqlite.pragma('journal_mode = WAL2');

// Sync data from PostgreSQL to SQLite
await initialSync(
  lc,
  {appID: 'debug_playlist_planner', shardNum: 0, publications: []},
  sqlite,
  getConnectionURI(pg),
  {tableCopyWorkers: 1},
);

// Run ANALYZE to populate SQLite statistics
sqlite.exec('ANALYZE;');

console.log('=== TABLE ROW COUNTS ===');
const playlistCount = sqlite
  .prepare('SELECT COUNT(*) as count FROM playlist')
  .get() as {count: number};
const playlistTrackCount = sqlite
  .prepare('SELECT COUNT(*) as count FROM playlist_track')
  .get() as {count: number};
const trackCount = sqlite
  .prepare('SELECT COUNT(*) as count FROM track')
  .get() as {count: number};

console.log(`Playlist: ${playlistCount.count}`);
console.log(`PlaylistTrack: ${playlistTrackCount.count}`);
console.log(`Track: ${trackCount.count}`);
console.log('');

// Create SQLite cost model
const costModel = createSQLiteCostModel(
  sqlite,
  Object.fromEntries(
    Object.entries(schema.tables).map(([k, v]) => [
      'serverName' in v ? v.serverName : k,
      {
        columns: Object.fromEntries(
          Object.entries(v.columns).map(([colName, col]) => [
            'serverName' in col ? col.serverName : colName,
            {
              ...col,
            },
          ]),
        ),
        primaryKey: v.primaryKey,
      },
    ]),
  ),
);

// Create name mappers
const clientToServerMapper = clientToServer(schema.tables);
const serverToClientMapper = serverToClient(schema.tables);

// Get the query AST directly from the builder
const unplannedAST = builder.playlist.whereExists('tracks').ast;

console.log('=== UNPLANNED AST ===');
console.log(JSON.stringify(unplannedAST, null, 2));
console.log('');

// Map to server names
const mappedAST = mapAST(unplannedAST, clientToServerMapper);

console.log('=== MAPPED AST (server names) ===');
console.log(JSON.stringify(mappedAST, null, 2));
console.log('');

// Create a cost model wrapper that logs all calls
const loggingCostModel = (
  table: string,
  sort: any,
  filters: any,
  constraint: any,
) => {
  const cost = costModel(table, sort, filters, constraint);
  console.log(`Cost for ${table}:`, {
    constraint,
    filters: filters ? 'present' : 'none',
    cost,
  });
  return cost;
};

console.log('=== PLANNING (with cost logging) ===');
const plannedServerAST = planQuery(mappedAST, loggingCostModel);
console.log('');

console.log('=== PLANNED AST (server names) ===');
console.log(JSON.stringify(plannedServerAST, null, 2));
console.log('');

// Map back to client names
const plannedClientAST = mapAST(plannedServerAST, serverToClientMapper);

console.log('=== PLANNED AST (client names) ===');
console.log(JSON.stringify(plannedClientAST, null, 2));
console.log('');

// Check flip decisions
function checkFlips(ast: any, path: string = 'root') {
  if (ast.where) {
    if ('flip' in ast.where) {
      console.log(`${path}.where.flip = ${ast.where.flip}`);
    }
    if (ast.where.related?.subquery?.where) {
      checkFlips(ast.where.related.subquery, `${path}.where.related.subquery`);
    }
    if (ast.where.conditions) {
      ast.where.conditions.forEach((c: any, i: number) => {
        if ('flip' in c) {
          console.log(`${path}.where.conditions[${i}].flip = ${c.flip}`);
        }
      });
    }
  }
}

console.log('=== FLIP DECISIONS ===');
checkFlips(plannedClientAST);
console.log('');

// Cleanup
await pg.end();
await fs.rm(tempDir, {recursive: true, force: true});
