import {consoleLogSink, LogContext} from '@rocicorp/logger';
import 'dotenv/config';
import {ident as id, literal} from 'pg-format';
import postgres from 'postgres';
import {parseOptions} from '../../../packages/shared/src/options.ts';
import * as v from '../../../packages/shared/src/valita.ts';

const options = {
  upstream: {
    db: v.string(),
  },

  qps: v.number().default(10),

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
  const {upstream, perturb, qps, maxConnections} = parseOptions(
    options,
    process.argv.slice(2),
    'ZERO_',
  );
  const db = postgres(upstream.db, {
    max: Math.max(1, Math.min(qps / 10, maxConnections)),
  });

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
  SELECT ${db(perturb.key)} FROM ${db(perturb.table)} LIMIT 10000`.values();
  lc.info?.(
    `Randomly perturbing ${keys.length} rows at ${qps} qps with ${stmt}?`,
  );

  let total = 0;
  let inFlight = 0;
  let running = true;
  process.on('SIGINT', () => (running = false));

  function logStatus() {
    process.stdout.write(`\rTOTAL: ${total}\tIN FLIGHT: ${inFlight}`);
  }
  const statusUpdater = setInterval(logStatus, 1000);

  function sendQueries() {
    const key = keys[Math.floor(Math.random() * keys.length)][0];
    inFlight++;
    void db
      .unsafe(stmt + literal(key))
      .execute()
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
