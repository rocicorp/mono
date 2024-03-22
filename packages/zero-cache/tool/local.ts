/**
 * Script to run replication logical locally. Run with `npm run local`,
 * optionally with Postgres Environment Variables to configure non-default
 * database connection values.
 *
 * https://www.postgresql.org/docs/current/libpq-envars.html
 *
 * Example:
 *
 * ```
 * $ PGPORT=5434 PGDATABASE=upstream npm run local
 * ```
 */

import {
  LogicalReplicationService,
  PgoutputPlugin,
} from 'pg-logical-replication';

const slotName = 'zero_slot';
const publicationNames = ['zero_data', 'zero_metadata'];

const service = new LogicalReplicationService(
  {},
  {acknowledge: {auto: false, timeoutSeconds: 0}},
);

const plugin = new PgoutputPlugin({
  protoVersion: 1,
  publicationNames,
});

service.on('data', (lsn, log) => {
  console.log(
    `"${lsn}": ${JSON.stringify(
      log,
      (_, v) => (typeof v === 'bigint' ? `BigInt(${v.toString()})` : v),
      2,
    )},`,
  );
});

service.on('error', err => {
  console.error('On error', err);
});

console.debug('Connecting ...');

(function proc() {
  void service
    .subscribe(plugin, slotName)
    .catch(e => console.error('Thrown error', e))
    .then(() => {
      console.log('Setting timeout');
      setTimeout(proc, 100);
    });

  console.debug('Connected!');
})();
