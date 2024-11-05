import {defineAuthorization} from '@rocicorp/zero/config';
import {type Schema, schema} from './src/domain/schema.js';

/** The contents of the zbugs JWT */
type AuthData = {
  // The logged in userID.
  sub: string;
};

export default defineAuthorization<AuthData, Schema>(schema, query => {
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
}) as ReturnType<typeof defineAuthorization>;
