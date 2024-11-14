import {append, TAGS} from './ddl.js';

export function dropEventTriggerStatements(shardID: string) {
  const sharded = append(shardID);
  const stmts = [`DROP EVENT TRIGGER IF EXISTS ${sharded('zero_ddl_start')};`];
  for (const tag of TAGS) {
    const tagID = tag.toLowerCase().replace(' ', '_');
    stmts.push(`DROP EVENT TRIGGER IF EXISTS ${sharded(`zero_${tagID}`)};`);
  }
  return stmts.join('');
}
