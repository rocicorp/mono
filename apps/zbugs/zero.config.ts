import {
  defineConfig,
  runtimeEnv,
} from '../../packages/zero-cache/src/config/define-config.js';
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
    upstreamDBConnStr: runtimeEnv('UPSTREAM_URI'),
    cvrDBConnStr: runtimeEnv('CVR_DB_URI'),
    changeDBConnStr: runtimeEnv('CHANGE_DB_URI'),

    numSyncWorkers: runtimeEnv('NUM_SYNC_WORKERS'),
    changeStreamerConnStr: runtimeEnv('CHANGE_STREAMER_URI'),

    replicaDBFile: runtimeEnv('REPLICA_DB_FILE'),
    jwtSecret: runtimeEnv('JWT_SECRET'),
    litestream: runtimeEnv('LITESTREAM'),
    warmWebsocket: 12,
    shard: {
      id: runtimeEnv('SHARD_ID'),
      publications: runtimeEnv('PUBLICATIONS'),
    },
    log: {
      level: runtimeEnv('LOG_LEVEL'),
      format: runtimeEnv('LOG_FORMAT'),
    },
    rateLimit: {
      mutationTransactions: {
        algorithm: 'sliding-window',
        // 100 writes per minute per user
        windowMs: 1000 * 60,
        maxTransactions: 100,
      },
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
