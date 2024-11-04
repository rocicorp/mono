import {defineConfig} from '@rocicorp/zero/config';
import {type Schema, schema} from './src/domain/schema.js';

/** The contents of the zbugs JWT */
type AuthData = {
  // The logged in userID.
  sub: string;
};

export default defineConfig<AuthData, Schema>(schema, query => {
  // TODO: We need `querify` so we can just check the authData without having to
  // read the DB E.g., `queries.querify(authData).where('sub', 'IS NOT', null)`
  const allowIfLoggedIn = (authData: AuthData) =>
    query.user.where('id', '=', authData.sub);

  const allowIfIssueCreator = (authData: AuthData, row: {id: string}) => {
    return query.issue
      .where('id', row.id)
      .where('creatorID', '=', authData.sub);
  };

  // TODO: It would be nice to share code with above.
  const allowIfCommentCreator = (authData: AuthData, row: {id: string}) => {
    return query.comment
      .where('id', row.id)
      .where('creatorID', '=', authData.sub);
  };

  const allowIfAdmin = (authData: AuthData) =>
    query.user.where('id', '=', authData.sub).where('role', '=', 'crew');

  return {
    upstreamDBConnStr: must(process.env['UPSTREAM_URI']),
    cvrDBConnStr: must(process.env['CVR_DB_URI']),
    changeDBConnStr: must(process.env['CHANGE_DB_URI']),

    numSyncWorkers:
      process.env['NUM_SYNC_WORKERS'] === undefined
        ? undefined // this means numCores - 1
        : parseInt(process.env['NUM_SYNC_WORKERS']),

    changeStreamerConnStr: process.env['CHANGE_STREAMER_URI'],

    replicaDBFile: must(process.env['REPLICA_DB_FILE']),
    jwtSecret: process.env['JWT_SECRET'],
    litestream: !!process.env['LITESTREAM'],
    shard: {
      id: process.env['SHARD_ID'] ?? '0',
      publications: process.env['PUBLICATIONS']
        ? process.env['PUBLICATIONS'].split(',')
        : [],
    },
    log: {
      level: process.env['LOG_LEVEL'] as 'debug' | 'info' | 'warn' | 'error',
      format: process.env['LOG_FORMAT'] as 'text' | 'json' | undefined,
    },
    perUserMutationLimit: {
      // 100 writes per minute per user
      windowMs: 1000 * 60,
      max: 100,
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
}) as ReturnType<typeof defineConfig>;

function must<T>(v: T | undefined | null): T {
  // eslint-disable-next-line eqeqeq
  if (v == null) {
    throw new Error(`Unexpected ${v} value`);
  }
  return v;
}
