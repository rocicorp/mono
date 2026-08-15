import type {AST} from '../../../zero-protocol/src/ast.ts';
import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import {
  clientSchemaFrom,
  createSchema,
} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  enumeration,
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {createBuilder} from '../../../zql/src/query/create-builder.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';

// Table definitions matching zbugs
const user = table('user')
  .columns({
    id: string(),
    login: string(),
    name: string().optional(),
    avatar: string(),
    role: enumeration<'user' | 'crew'>(),
  })
  .primaryKey('id')
  .unique('login');

const project = table('project')
  .columns({
    id: string(),
    name: string(),
    lowerCaseName: string(),
    issueCountEstimate: number().optional(),
    supportsSearch: boolean(),
    markURL: string().optional(),
    logoURL: string().optional(),
  })
  .primaryKey('id')
  .unique('name')
  .unique('lowerCaseName');

const issue = table('issue')
  .columns({
    id: string(),
    shortID: number().optional(),
    title: string(),
    open: boolean(),
    modified: number(),
    created: number(),
    projectID: string(),
    creatorID: string(),
    assigneeID: string().optional(),
    description: string(),
    visibility: enumeration<'internal' | 'public'>(),
  })
  .primaryKey('id');

const viewState = table('viewState')
  .columns({
    issueID: string(),
    userID: string(),
    viewed: number(),
  })
  .primaryKey('userID', 'issueID');

const comment = table('comment')
  .columns({
    id: string(),
    issueID: string(),
    created: number(),
    body: string(),
    creatorID: string(),
  })
  .primaryKey('id');

const label = table('label')
  .columns({
    id: string(),
    name: string(),
    projectID: string(),
  })
  .primaryKey('id')
  .unique('projectID', 'name');

const issueLabel = table('issueLabel')
  .columns({
    issueID: string(),
    labelID: string(),
    projectID: string(),
  })
  .primaryKey('labelID', 'issueID');

const emoji = table('emoji')
  .columns({
    id: string(),
    value: string(),
    annotation: string(),
    subjectID: string(),
    creatorID: string(),
    created: number(),
  })
  .primaryKey('id');

const userPref = table('userPref')
  .columns({
    key: string(),
    userID: string(),
    value: string(),
  })
  .primaryKey('userID', 'key');

const issueNotifications = table('issueNotifications')
  .columns({
    userID: string(),
    issueID: string(),
    subscribed: boolean(),
    created: number(),
  })
  .primaryKey('userID', 'issueID');

// Relationships
const userRelationships = relationships(user, ({many}) => ({
  createdIssues: many({
    sourceField: ['id'],
    destField: ['creatorID'],
    destSchema: issue,
  }),
  assignedIssues: many({
    sourceField: ['id'],
    destField: ['assigneeID'],
    destSchema: issue,
  }),
}));

const projectRelationships = relationships(project, ({many}) => ({
  issues: many({
    sourceField: ['id'],
    destField: ['projectID'],
    destSchema: issue,
  }),
  labels: many({
    sourceField: ['id'],
    destField: ['projectID'],
    destSchema: label,
  }),
}));

const issueRelationships = relationships(issue, ({many, one}) => ({
  project: one({
    sourceField: ['projectID'],
    destField: ['id'],
    destSchema: project,
  }),
  issueLabels: many({
    sourceField: ['id'],
    destField: ['issueID'],
    destSchema: issueLabel,
  }),
  labels: many(
    {
      sourceField: ['id'],
      destField: ['issueID'],
      destSchema: issueLabel,
    },
    {
      sourceField: ['labelID'],
      destField: ['id'],
      destSchema: label,
    },
  ),
  comments: many({
    sourceField: ['id'],
    destField: ['issueID'],
    destSchema: comment,
  }),
  creator: one({
    sourceField: ['creatorID'],
    destField: ['id'],
    destSchema: user,
  }),
  assignee: one({
    sourceField: ['assigneeID'],
    destField: ['id'],
    destSchema: user,
  }),
  viewState: many({
    sourceField: ['id'],
    destField: ['issueID'],
    destSchema: viewState,
  }),
  emoji: many({
    sourceField: ['id'],
    destField: ['subjectID'],
    destSchema: emoji,
  }),
  notificationState: one({
    sourceField: ['id'],
    destField: ['issueID'],
    destSchema: issueNotifications,
  }),
}));

const commentRelationships = relationships(comment, ({one, many}) => ({
  creator: one({
    sourceField: ['creatorID'],
    destField: ['id'],
    destSchema: user,
  }),
  emoji: many({
    sourceField: ['id'],
    destField: ['subjectID'],
    destSchema: emoji,
  }),
  issue: one({
    sourceField: ['issueID'],
    destField: ['id'],
    destSchema: issue,
  }),
}));

const issueLabelRelationships = relationships(issueLabel, ({one}) => ({
  issue: one({
    sourceField: ['issueID'],
    destField: ['id'],
    destSchema: issue,
  }),
  label: one({
    sourceField: ['labelID'],
    destField: ['id'],
    destSchema: label,
  }),
}));

const labelRelationships = relationships(label, ({one}) => ({
  project: one({
    sourceField: ['projectID'],
    destField: ['id'],
    destSchema: project,
  }),
}));

const emojiRelationships = relationships(emoji, ({one}) => ({
  creator: one({
    sourceField: ['creatorID'],
    destField: ['id'],
    destSchema: user,
  }),
  issue: one({
    sourceField: ['subjectID'],
    destField: ['id'],
    destSchema: issue,
  }),
  comment: one({
    sourceField: ['subjectID'],
    destField: ['id'],
    destSchema: comment,
  }),
}));

export const schema = createSchema({
  tables: [
    user,
    project,
    issue,
    comment,
    label,
    issueLabel,
    viewState,
    emoji,
    userPref,
    issueNotifications,
  ],
  relationships: [
    userRelationships,
    projectRelationships,
    issueRelationships,
    commentRelationships,
    issueLabelRelationships,
    labelRelationships,
    emojiRelationships,
  ],
});

export const builder = createBuilder(schema);

/**
 * Returns the normalized client schema and hash for zbugs.
 */
export function getZbugsClientSchema() {
  return clientSchemaFrom(schema);
}

/**
 * Pre-built ASTs modeling realistic zbugs query patterns:
 * 1. Open issues ordered by modified (list page)
 * 2. All issues with creator and assignee
 * 3. Recent comments stream
 * 4. Projects with labels
 * 5. Users directory
 */
export function getDefaultZbugsQueries(): AST[] {
  const openIssuesList = asQueryInternals(
    builder.issue
      .where('open', true)
      .related('labels')
      .related('creator')
      .related('project')
      .orderBy('modified', 'desc')
      .limit(50),
  ).ast;

  const allIssues = asQueryInternals(
    builder.issue
      .related('creator')
      .related('assignee')
      .related('labels')
      .orderBy('created', 'desc')
      .limit(100),
  ).ast;

  const recentComments = asQueryInternals(
    builder.comment
      .related('creator')
      .related('issue')
      .orderBy('created', 'desc')
      .limit(50),
  ).ast;

  const projectsWithLabels = asQueryInternals(
    builder.project.related('labels'),
  ).ast;

  const users = asQueryInternals(
    builder.user.orderBy('login', 'asc').limit(100),
  ).ast;

  return [openIssuesList, allIssues, recentComments, projectsWithLabels, users];
}
