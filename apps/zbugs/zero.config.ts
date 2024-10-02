import 'dotenv/config';
import {defineConfig, runtimeEnv} from 'zero-cache/src/config/define-config.js';
import {schema, type Schema} from './src/domain/schema.js';

/** The contents of the decoded JWT */
type AuthData = {
  // The logged in userID.
  sub: string;
};

defineConfig<AuthData, Schema>(schema, queries => {
  // TODO: We need `querify` so we can just check the authData without having to
  // read the DB E.g., `queries.querify(authData).where('sub', 'IS NOT', null)`
  const allowIfLoggedIn = (authData: AuthData) =>
    queries.user.where('id', '=', authData.sub);

  const allowIfCreatorOf =
    (table: 'issue' | 'comment') => (authData: AuthData, row: {id: string}) => {
      // Sigh, TypeScript
      // Why can't I treat these generically?
      if (table === 'issue') {
        return queries[table]
          .where('id', row.id)
          .where('creatorID', '=', authData.sub);
      } else {
        return queries[table]
          .where('id', row.id)
          .where('creatorID', '=', authData.sub);
      }
    };

  const allowIfCrewMember = (authData: AuthData) =>
    queries.user.where('id', '=', authData.sub).where('role', '=', 'crew');

  return {
    upstreamUri: runtimeEnv('UPSTREAM_URI'),
    cvrDbUri: runtimeEnv('CVR_DB_URI'),
    changeDbUri: runtimeEnv('CHANGE_DB_URI'),

    replicaDbFile: runtimeEnv('REPLICA_DB_FILE'),
    jwtSecret: runtimeEnv('JWT_SECRET'),
    litestream: runtimeEnv('LITESTREAM'),
    shard: {
      id: runtimeEnv('SHARD_ID'),
      publications: runtimeEnv('PUBLICATIONS'),
    },
    log: {
      level: 'debug',
    },

    authorization: {
      user: {
        // Only the authentication system can write to the user table.
        table: {
          insert: [],
          update: [],
          delete: [],
        },
      },
      issue: {
        row: {
          insert: [allowIfLoggedIn],
          update: [allowIfCreatorOf('issue'), allowIfCrewMember],
          delete: [allowIfCreatorOf('issue'), allowIfCrewMember],
        },
      },
      comment: {
        row: {
          insert: [allowIfLoggedIn],
          update: [allowIfCreatorOf('comment'), allowIfCrewMember],
          delete: [allowIfCreatorOf('comment'), allowIfCrewMember],
        },
      },
    },
  };
});
