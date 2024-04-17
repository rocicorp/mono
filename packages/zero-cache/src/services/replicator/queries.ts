import type postgres from 'postgres';
import type {LexiVersion} from '../../types/lexi-version.js';

export function queryStateVersion(db: postgres.Sql) {
  return db<
    {max: LexiVersion | null}[]
  >`SELECT MAX("stateVersion") FROM _zero."TxLog";`;
}
