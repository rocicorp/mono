import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {nanoid} from 'nanoid/non-secure';
import {ident as id, literal} from 'pg-format';
import postgres from 'postgres';
import {parseOptions} from '../../../packages/shared/src/options.ts';
import * as v from '../../../packages/shared/src/valita.ts';
import '../../shared/src/dotenv.ts';

const options = {
  upstream: {
    db: v.string(),
  },

  qps: v.number().default(10),

  // Insert new rows by copying existing rows using random keys.
  insert: {
    table: v.string().default('issue'),
    key: v.string().default('id'),
    batch: v.number().optional(),
  },

  // Otherwise, if --insert-batch is unspecified, perturb existing rows.
  perturb: {
    table: v.string().default('issue'),
    key: v.string().default('id'),
    bools: v.array(v.string()).optional(),
    ints: v.array(v.string()).optional(),
    jsonbs: v.array(v.string()).optional(),
  },

  maxConnections: v.number().default(40),
};

async function run() {
  const lc = new LogContext('debug', {}, consoleLogSink);
  const {upstream, insert, perturb, qps, maxConnections} = parseOptions(
    options,
    {envNamePrefix: 'ZERO_'},
  );
  const db = postgres(upstream.db, {
    max: Math.max(1, Math.min(maxConnections, qps / 10)),
  });

  let sendQuery: () => Promise<unknown>;
  if (insert.batch) {
    lc.info?.(`Looking up ${insert.table} rows`);
    const {batch} = insert;
    const rows = await db`
    SELECT * FROM ${db(insert.table)} LIMIT 100000`;
    lc.info?.(
      `Inserting copies of ${rows.length} rows in random batches of ${batch} at ${qps} qps`,
    );

    sendQuery = () =>
      db.begin(async tx => {
        for (let i = 0; i < batch; i++) {
          const row = rows[Math.floor(Math.random() * rows.length)];
          const newRow = {
            ...row,
            [insert.key]: nanoid(10),
            visibility: 'public',
          };
          await tx`INSERT INTO ${tx(insert.table)} ${tx(newRow)}`;
        }
      });
  } else {
    const assignments = [`${id(perturb.key)} = ${id(perturb.key)}`];
    perturb.bools?.forEach(col =>
      assignments.push(`${id(col)} = NOT ${id(col)}`),
    );
    perturb.ints?.forEach(col =>
      assignments.push(`${id(col)} = ${id(col)} + FLOOR(RANDOM() * 2) - 1`),
    );
    perturb.jsonbs?.forEach(col =>
      assignments.push(
        `${id(col)} = jsonb_set(
         ${id(col)}, '{_rand}'::text[], (random()::text)::jsonb, true
       )`,
      ),
    );
    const stmt = `
    UPDATE ${id(perturb.table)} 
      SET ${assignments.join(',')} 
      WHERE ${id(perturb.key)} = `;

    lc.info?.(`Looking up ${perturb.table} ids`);
    const keys = await db<{key: string}[]>`
    SELECT ${db(perturb.key)} FROM ${db(perturb.table)} LIMIT 100000`.values();
    lc.info?.(
      `Randomly perturbing ${keys.length} rows at ${qps} qps with ${stmt}?`,
    );
    sendQuery = () => {
      const key = keys[Math.floor(Math.random() * keys.length)][0];
      return db.unsafe(stmt + literal(key));
    };
  }

  let total = 0;
  let inFlight = 0;
  let running = true;
  process.on('SIGINT', () => (running = false));

  function logStatus() {
    process.stdout.write(`\rTOTAL: ${total}\tIN FLIGHT: ${inFlight}`);
  }
  const statusUpdater = setInterval(logStatus, 1000);

  function sendQueries() {
    inFlight++;
    void sendQuery()
      .then(() => {
        total++;
        inFlight--;
      })
      .catch(lc.error);
    if (running) {
      setTimeout(sendQueries, 1000 / qps);
    } else {
      clearInterval(statusUpdater);
      lc.info?.();
      void db.end();
    }
  }
  sendQueries();
}

await run();
