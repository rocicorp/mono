import 'dotenv/config';

import {getZeroConfig} from '../config/zero-config.js';
import {getSchema} from '../auth/load-schema.js';
import {must} from '../../../shared/src/must.js';
import {Database} from '../../../zqlite/src/db.js';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.js';
import {type QueryDelegate} from '../../../zql/src/query/query-impl.js';
import {TableSource} from '../../../zqlite/src/table-source.js';
import {normalizeSchema} from '../../../zero-schema/src/normalized-schema.js';
import {MemoryStorage} from '../../../zql/src/ivm/memory-storage.js';
import type {AST} from '../../../zero-protocol/src/ast.js';
import {buildPipeline} from '../../../zql/src/builder/builder.js';
import {Catch} from '../../../zql/src/ivm/catch.js';

const config = getZeroConfig();
const schema = await getSchema(config);
const normalizedSchema = normalizeSchema(schema.schema);

const ast = JSON.parse(must(config.ast)) as AST;

const db = new Database(createSilentLogContext(), config.replicaFile);
const sources = new Map<string, TableSource>();
const host: QueryDelegate = {
  getSource: (name: string) => {
    let source = sources.get(name);
    if (source) {
      return source;
    }
    source = new TableSource(
      db,
      name,
      normalizedSchema.tables[name].columns,
      normalizedSchema.tables[name].primaryKey,
    );

    sources.set(name, source);
    return source;
  },

  createStorage() {
    // TODO: table storage!!
    return new MemoryStorage();
  },
  addServerQuery() {
    return () => {};
  },
  onTransactionCommit() {
    return () => {};
  },
  batchViewUpdates<T>(applyViewUpdates: () => T): T {
    return applyViewUpdates();
  },
};

const pipeline = buildPipeline(ast, host);
const output = new Catch(pipeline);

const start = performance.now();
const view = output.fetch();
const end = performance.now();

let totalRowsConsidered = 0;
for (const source of sources.values()) {
  totalRowsConsidered += source.rowsVended;
}

console.log(JSON.stringify(view, null, 2));
console.log('ROWS CONSIDERED:', totalRowsConsidered);
console.log('TIME:', (end - start).toFixed(2), 'ms');

/*
Command:
npm run run-query -- --ast 

Bad query:
{"table":"issue","orderBy":[["id","asc"]],"related":[{"subquery":{"alias":"assignee","table":"user","orderBy":[["id","asc"]]},"correlation":{"childField":["id"],"parentField":["assigneeID"]}},{"subquery":{"alias":"comments","limit":10,"table":"comment","orderBy":[["id","asc"]],"related":[{"subquery":{"alias":"emoji","table":"emoji","orderBy":[["id","asc"]]},"correlation":{"childField":["subjectID"],"parentField":["id"]}}],"where":{"type":"correlatedSubquery","related":{"correlation":{"parentField":["issueID"],"childField":["id"]},"subquery":{"table":"issue","alias":"zsubq_issue","where":{"type":"or","conditions":[{"type":"simple","left":{"type":"literal","value":null},"right":{"type":"literal","value":"crew"},"op":"="},{"type":"simple","left":{"type":"column","name":"visibility"},"right":{"type":"literal","value":"public"},"op":"="}]},"orderBy":[["id","asc"]]}},"op":"EXISTS"}},"correlation":{"childField":["issueID"],"parentField":["id"]}},{"subquery":{"alias":"creator","table":"user","orderBy":[["id","asc"]]},"correlation":{"childField":["id"],"parentField":["creatorID"]}},{"subquery":{"alias":"emoji","table":"emoji","orderBy":[["id","asc"]]},"correlation":{"childField":["subjectID"],"parentField":["id"]}},{"subquery":{"alias":"labels","table":"issueLabel","orderBy":[["issueID","asc"],["labelID","asc"]],"related":[{"hidden":true,"subquery":{"alias":"labels","table":"label","orderBy":[["id","asc"]]},"correlation":{"childField":["id"],"parentField":["labelID"]}}],"where":{"type":"correlatedSubquery","related":{"correlation":{"parentField":["issueID"],"childField":["id"]},"subquery":{"table":"issue","alias":"zsubq_issue","where":{"type":"or","conditions":[{"type":"simple","left":{"type":"literal","value":null},"right":{"type":"literal","value":"crew"},"op":"="},{"type":"simple","left":{"type":"column","name":"visibility"},"right":{"type":"literal","value":"public"},"op":"="}]},"orderBy":[["id","asc"]]}},"op":"EXISTS"}},"correlation":{"childField":["issueID"],"parentField":["id"]}},{"subquery":{"alias":"viewState","limit":1,"table":"viewState","where":{"op":"=","left":{"name":"userID","type":"column"},"type":"simple","right":{"type":"literal","value":"tDY6IbKdVqbBlRBc3XMwF"}},"orderBy":[["userID","asc"],["issueID","asc"]]},"correlation":{"childField":["issueID"],"parentField":["id"]}}],"where":{"type":"or","conditions":[{"type":"simple","left":{"type":"literal","value":null},"right":{"type":"literal","value":"crew"},"op":"="},{"type":"simple","left":{"type":"column","name":"visibility"},"right":{"type":"literal","value":"public"},"op":"="}]}}
ROWS CONSIDERED: 12623
TIME: 234.32 ms

Query with no read perms:
{"table":"issue","orderBy":[["id","asc"]],"related":[{"subquery":{"alias":"assignee","table":"user","orderBy":[["id","asc"]]},"correlation":{"childField":["id"],"parentField":["assigneeID"]}},{"subquery":{"alias":"comments","limit":10,"table":"comment","orderBy":[["id","asc"]],"related":[{"subquery":{"alias":"emoji","table":"emoji","orderBy":[["id","asc"]]},"correlation":{"childField":["subjectID"],"parentField":["id"]}}]},"correlation":{"childField":["issueID"],"parentField":["id"]}},{"subquery":{"alias":"creator","table":"user","orderBy":[["id","asc"]]},"correlation":{"childField":["id"],"parentField":["creatorID"]}},{"subquery":{"alias":"emoji","table":"emoji","orderBy":[["id","asc"]]},"correlation":{"childField":["subjectID"],"parentField":["id"]}},{"subquery":{"alias":"labels","table":"issueLabel","orderBy":[["issueID","asc"],["labelID","asc"]],"related":[{"hidden":true,"subquery":{"alias":"labels","table":"label","orderBy":[["id","asc"]]},"correlation":{"childField":["id"],"parentField":["labelID"]}}]},"correlation":{"childField":["issueID"],"parentField":["id"]}},{"subquery":{"alias":"viewState","limit":1,"table":"viewState","where":{"op":"=","left":{"name":"userID","type":"column"},"type":"simple","right":{"type":"literal","value":"tDY6IbKdVqbBlRBc3XMwF"}},"orderBy":[["userID","asc"],["issueID","asc"]]},"correlation":{"childField":["issueID"],"parentField":["id"]}}],"where":{"type":"or","conditions":[{"type":"simple","left":{"type":"literal","value":null},"right":{"type":"literal","value":"crew"},"op":"="},{"type":"simple","left":{"type":"column","name":"visibility"},"right":{"type":"literal","value":"public"},"op":"="}]}}
ROWS CONSIDERED: 3655
TIME: 86.58 ms
*/
