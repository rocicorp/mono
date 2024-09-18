import process from 'node:process';
import 'dotenv/config';
import {defineConfig} from 'zero-cache/src/config/define-config.js';
import {must} from 'shared/src/must';

type AuthData = {aud: string};
type TODO = any;
defineConfig<AuthData, TODO>({
  upstreamUri: must(process.env.UPSTREAM_URI),
  cvrDbUri: must(process.env.CVR_DB_URI),
  changeDbUri: must(process.env.CHANGE_DB_URI),

  replicaId: must(process.env.REPLICA_ID),
  replicaDbFile: must(process.env.REPLICA_DB_FILE),

  log: {
    level: 'debug',
  },
});
