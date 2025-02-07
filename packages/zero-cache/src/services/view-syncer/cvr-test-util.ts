import type {PostgresDB} from '../../types/pg.ts';
import {
  type ClientsRow,
  type DesiresRow,
  type InstancesRow,
  type QueriesRow,
  type RowsRow,
  type RowsVersionRow,
} from './schema/cvr.ts';

export type DBState = {
  instances: (Partial<InstancesRow> &
    Pick<InstancesRow, 'clientGroupID' | 'version'>)[];
  clients: ClientsRow[];
  queries: QueriesRow[];
  desires: DesiresRow[];
  rows: RowsRow[];
  rowsVersion?: RowsVersionRow[];
};

export function setInitialState(
  prefix: string,
  db: PostgresDB,
  state: Partial<DBState>,
): Promise<void> {
  return db.begin(async tx => {
    const {instances, rowsVersion} = state;
    if (instances && !rowsVersion) {
      state = {
        ...state,
        rowsVersion: instances.map(({clientGroupID, version}) => ({
          clientGroupID,
          version,
        })),
      };
    }
    for (const [table, rows] of Object.entries(state)) {
      for (const row of rows) {
        await tx`INSERT INTO ${tx(prefix + '.' + table)} ${tx(row)}`;
      }
    }
  });
}
