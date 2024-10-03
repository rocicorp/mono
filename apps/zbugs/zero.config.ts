import {defineConfig, runtimeEnv} from 'zero-cache/src/config/define-config.js';
import {schema, type Schema} from './src/domain/schema.js';

/** The contents of the zbugs JWT */
type AuthData = {
  // The logged in userID.
  sub: string;
};

defineConfig<AuthData, Schema>(schema, queries => {
  // TODO: We need `querify` so we can just check the authData without having to
  // read the DB E.g., `queries.querify(authData).where('sub', 'IS NOT', null)`
  const allowIfLoggedIn = (authData: AuthData) =>
    queries.user.where('id', '=', authData.sub);

  const allowIfIssueCreator = (authData: AuthData, row: {id: string}) => {
    return queries.issue
      .where('id', row.id)
      .where('creatorID', '=', authData.sub);
  };

  // TODO: It would be nice to share code with above.
  const allowIfCommentCreator = (authData: AuthData, row: {id: string}) => {
    return queries.comment
      .where('id', row.id)
      .where('creatorID', '=', authData.sub);
  };

  const allowIfAdmin = (authData: AuthData) =>
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
          update: [allowIfIssueCreator, allowIfAdmin],
          delete: [allowIfIssueCreator, allowIfAdmin],
        },
      },
      comment: {
        row: {
          insert: [allowIfLoggedIn],
          update: [allowIfCommentCreator, allowIfAdmin],
          delete: [allowIfCommentCreator, allowIfAdmin],
        },
      },
    },
  };
});
