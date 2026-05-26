import type {Kysely} from 'kysely';
import {describe, expect, test, vi} from 'vitest';
import type {Row} from '../../../zql/src/mutate/custom.ts';
import {KyselyConnection} from './kysely.ts';

describe('KyselyConnection', () => {
  test('passes raw sql and parameters through executeQuery', async () => {
    const sampleRows: Row[] = [{id: 1}];
    const executeQuery = vi.fn().mockResolvedValue({rows: sampleRows});
    const fakeTransaction = {executeQuery};
    const client = {
      transaction() {
        return {
          execute<T>(callback: (tx: typeof fakeTransaction) => Promise<T>) {
            return callback(fakeTransaction);
          },
        };
      },
    } as unknown as Kysely<unknown>;

    const connection = new KyselyConnection(client);
    const rows = await connection.transaction(tx =>
      tx.query('SELECT * FROM "user" WHERE id = $1', [123]),
    );

    expect(rows).toStrictEqual(sampleRows);
    expect(executeQuery).toHaveBeenCalledTimes(1);
    expect(executeQuery).toHaveBeenCalledWith(
      expect.objectContaining({
        sql: 'SELECT * FROM "user" WHERE id = $1',
        parameters: [123],
      }),
    );
  });
});
