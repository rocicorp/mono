import * as v from 'shared/src/valita.js';
import {cfCall} from './fetch.js';
import type {AccountAccess} from './resources.js';
import type {SelectSchema, SelectStatement} from './sql.js';

export class Analytics {
  readonly #apiToken: string;
  readonly #resource: string;

  constructor({apiToken, accountID}: AccountAccess) {
    this.#apiToken = apiToken;
    this.#resource = `/accounts/${accountID}/analytics_engine/sql`;
  }

  async query<T extends SelectSchema>(statement: SelectStatement<T>) {
    const body = statement.toString();
    console.info(`QUERY: ${body}`);
    const resp = await cfCall(this.#apiToken, this.#resource, {
      method: 'POST',
      body,
    });
    const json = await resp.json();
    return v.parse(json, queryResultSchema(statement.schema), 'passthrough');
  }
}

// Structure of the JSON response to a SELECT {row} query.
export function queryResultSchema<T>(row: v.Type<T>) {
  return v.object({
    meta: v.array(
      v.object({
        name: v.string(),
        type: v.string(),
      }),
    ),
    data: v.array(row),
    rows: v.number(),
    // eslint-disable-next-line @typescript-eslint/naming-convention
    rows_before_limit_at_least: v.number(),
  });
}
