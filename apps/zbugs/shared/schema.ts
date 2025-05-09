import {
  ANYONE_CAN,
  createSchema,
  definePermissions,
  relationships,
  type ExpressionBuilder,
} from '@rocicorp/zero';
import type {AuthData} from './auth.ts';
import {
  commentTable,
  commentTableRelationships,
  emojiTable,
  emojiTableRelationships,
  issueLabelTable,
  issueLabelTableRelationships,
  issueTable,
  issueTableRelationships,
  labelTable,
  userPrefTable,
  userTable,
  userTableRelationships,
  viewStateTable,
} from './prisma/generated/schema.ts';

export type {
  Comment as CommentRow,
  Issue as IssueRow,
  User as UserRow,
} from './prisma/generated/schema.ts';

// Prisma cannot generate the labels relationships because of the way
// join/junction tables work there is different.
const manualIssueTableRelationships = relationships(issueTable, ({many}) => ({
  ...issueTableRelationships.relationships,
  labels: many(
    {
      sourceField: ['id'],
      destField: ['issueID'],
      destSchema: issueLabelTable,
    },
    {
      sourceField: ['labelID'],
      destField: ['id'],
      destSchema: labelTable,
    },
  ),
}));

export const schema = createSchema({
  tables: [
    userTable,
    issueTable,
    commentTable,
    labelTable,
    issueLabelTable,
    viewStateTable,
    emojiTable,
    userPrefTable,
  ],
  relationships: [
    userTableRelationships,
    manualIssueTableRelationships,
    commentTableRelationships,
    issueLabelTableRelationships,
    emojiTableRelationships,
  ],
});

export type Schema = typeof schema;
type TableName = keyof Schema['tables'];

export const permissions: ReturnType<typeof definePermissions> =
  definePermissions<AuthData, Schema>(schema, () => {
    const userIsLoggedIn = (
      authData: AuthData,
      {cmpLit}: ExpressionBuilder<Schema, TableName>,
    ) => cmpLit(authData.sub, 'IS NOT', null);

    const loggedInUserIsAdmin = (
      authData: AuthData,
      eb: ExpressionBuilder<Schema, TableName>,
    ) =>
      eb.and(
        userIsLoggedIn(authData, eb),
        eb.cmpLit(authData.role, '=', 'crew'),
      );

    const allowIfUserIDMatchesLoggedInUser = (
      authData: AuthData,
      {cmp}: ExpressionBuilder<Schema, 'viewState' | 'userPref'>,
    ) => cmp('userID', '=', authData.sub);

    const canSeeIssue = (
      authData: AuthData,
      eb: ExpressionBuilder<Schema, 'issue'>,
    ) =>
      eb.or(loggedInUserIsAdmin(authData, eb), eb.cmp('visibility', 'public'));

    return {
      user: {
        row: {
          select: ANYONE_CAN,
        },
      },
      issue: {
        row: {
          select: [canSeeIssue],
        },
      },
      comment: {
        row: {
          select: ANYONE_CAN,
        },
      },
      label: {
        row: {
          select: ANYONE_CAN,
        },
      },
      viewState: {
        row: {
          select: ANYONE_CAN,
        },
      },
      issueLabel: {
        row: {
          select: ANYONE_CAN,
        },
      },
      emoji: {
        row: {
          select: ANYONE_CAN,
        },
      },
      userPref: {
        row: {
          select: [allowIfUserIDMatchesLoggedInUser],
        },
      },
    };
  });
