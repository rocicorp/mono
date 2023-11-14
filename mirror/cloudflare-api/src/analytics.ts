import * as v from 'shared/src/valita.js';
import {cfCall} from './fetch.js';
import type {AccountAccess} from './resources.js';
import type {SelectSchema, SelectStatement} from './sql.js';

export class Analytics {
  readonly #apiToken: string;
  readonly #resource: string;
  readonly accountID: string;

  constructor({apiToken, accountID}: AccountAccess) {
    this.#apiToken = apiToken;
    this.accountID = accountID;
    this.#resource = `/accounts/${accountID}/analytics_engine/sql`;
  }

  async query<T extends SelectSchema>(statement: SelectStatement<T>) {
    const resp = await this.#query(statement.toString());
    const json = await resp.json();
    return v.parse(json, queryResultSchema(statement.schema), 'passthrough');
  }

  async queryRaw(statement: string): Promise<string> {
    const resp = await this.#query(statement);
    return resp.text();
  }

  async #query(statement: string) {
    console.info(`QUERY: ${statement}`);
    const resp = await cfCall(this.#apiToken, this.#resource, {
      method: 'POST',
      body: statement,
    });
    if (!resp.ok) {
      throw new Error(
        `${this.#resource}: ${resp.status}: ${await resp.text()}`,
      );
    }
    return resp;
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
