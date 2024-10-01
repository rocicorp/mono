import 'dotenv/config';
import {
  defineConfig,
  runtimeEnv,
  type Queries,
} from 'zero-cache/src/config/define-config.js';
import {type Schema, schema} from './src/domain/schema.js';

type AuthData = {sub: string};

const allowIfCrewMember = (queries: Queries<Schema>) => (authData: AuthData) =>
  queries.user.where('id', '=', authData.sub).where('role', '=', 'crew');

// TODO:
// 1. The double lambda is annoying
// 2. We need `querify` so we can just check the authData without having to read the DB
// E.g., `queries.querify(authData).where('sub', 'IS NOT', null)`
const allowIfLoggedIn = (queries: Queries<Schema>) => (authData: AuthData) =>
  queries.user.where('id', '=', authData.sub);

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
        insert: [allowIfLoggedIn(queries)],
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
        insert: [allowIfLoggedIn(queries)],
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
