import 'dotenv/config';
import {
  defineConfig,
  runtimeEnv,
  type Queries,
} from 'zero-cache/src/config/define-config.js';
import {type Schema, schema} from './src/domain/schema-shared.js';

type AuthData = {sub: string};

const allowIfCrewMember = (queries: Queries<Schema>) => (authData: AuthData) =>
  queries.user.where('id', '=', authData.sub).where('role', '=', 'crew');

defineConfig<AuthData, Schema>(schema, queries => ({
  upstreamUri: runtimeEnv('UPSTREAM_URI'),
  cvrDbUri: runtimeEnv('CVR_DB_URI'),
  changeDbUri: runtimeEnv('CHANGE_DB_URI'),

  replicaId: runtimeEnv('REPLICA_ID'),
  replicaDbFile: runtimeEnv('REPLICA_DB_FILE'),
  jwtSecret: runtimeEnv('JWT_SECRET'),
  litestream: runtimeEnv('LITESTREAM'),

  log: {
    level: 'debug',
  },

  authorization: {
    user: {
      // Only the authentication system can
      // write to the user table.
      table: {
        delete: [],
        insert: [],
        update: [],
      },
    },
    issue: {
      row: {
        delete: [],
        update: [
          (authData, row) =>
            queries.issue
              .where('id', '=', row.id)
              .where('creatorID', '=', authData.sub),
          allowIfCrewMember(queries),
        ],
      },
    },
    comment: {
      row: {
        delete: [],
        update: [
          (authData, row) =>
            queries.comment
              .where('id', '=', row.id)
              .where('creatorID', '=', authData.sub),
        ],
      },
    },
  },
}));
