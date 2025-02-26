import {append, TAGS} from './ddl.ts';

export function dropEventTriggerStatements(appID: string, shardID: string) {
  const sharded = append(shardID);
  const stmts = [
    `DROP EVENT TRIGGER IF EXISTS ${sharded(`${appID}_ddl_start`)};`,
  ];
  for (const tag of TAGS) {
    const tagID = tag.toLowerCase().replace(' ', '_');
    stmts.push(`DROP EVENT TRIGGER IF EXISTS ${sharded(`${appID}_${tagID}`)};`);
  }
  return stmts.join('');
}
