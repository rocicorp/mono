import {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {assert} from '../../../shared/src/asserts.ts';
import {h128} from '../../../shared/src/hash.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {must} from '../../../shared/src/must.ts';
import type {
  DeleteOp,
  InsertOp,
  UpdateOp,
} from '../../../zero-protocol/src/push.ts';
import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {
  ANYONE_CAN,
  ANYONE_CAN_DO_ANYTHING,
  definePermissions,
} from '../../../zero-schema/src/permissions.ts';
import type {ValueType} from '../../../zero-schema/src/table-schema.ts';
import type {Schema as ZeroSchema} from '../../../zero-types/src/schema.ts';
import {
  bindStaticParameters,
  buildPipeline,
} from '../../../zql/src/builder/builder.ts';
import {Catch, type CaughtNode} from '../../../zql/src/ivm/catch.ts';
import {consume} from '../../../zql/src/ivm/stream.ts';
import type {Source} from '../../../zql/src/ivm/source.ts';
import type {ExpressionBuilder} from '../../../zql/src/query/expression.ts';
import type {QueryBuilder, Row} from '../../../zql/src/query/query-builder.ts';
import {QueryDelegateBase} from '../../../zql/src/query/query-delegate-base.ts';
import type {QueryDelegate} from '../../../zql/src/query/query-delegate.ts';
import {newQuery} from '../../../zql/src/query/query-impl.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../zqlite/src/database-storage.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {TableSource} from '../../../zqlite/src/table-source.ts';
import type {ZeroConfig} from '../config/zero-config.ts';
import {transformQuery} from './read-authorizer.ts';
import {WriteAuthorizerImpl} from './write-authorizer.ts';

const zeroConfig = {
  log: testLogConfig,
} as unknown as ZeroConfig;

const user = table('user')
  .columns({
    id: string(),
    name: string(),
    role: string(),
  })
  .primaryKey('id');

const issue = table('issue')
  .columns({
    id: string(),
    title: string(),
    description: string(),
    closed: boolean(),
    ownerId: string(),
    creatorId: string(),
    projectId: string(),
  })
  .primaryKey('id');

const comment = table('comment')
  .columns({
    id: string(),
    issueId: string(),
    authorId: string(),
    text: string(),
  })
  .primaryKey('id');

const issueLabel = table('issueLabel')
  .columns({
    issueId: string(),
    labelId: string(),
  })
  .primaryKey('issueId', 'labelId');

const label = table('label')
  .columns({
    id: string(),
    name: string(),
  })
  .primaryKey('id');

const viewState = table('viewState')
  .columns({
    userId: string(),
    issueId: string(),
    lastRead: number(),
  })
  .primaryKey('issueId', 'userId');

const project = table('project')
  .columns({
    id: string(),
    name: string(),
  })
  .primaryKey('id');

const projectMember = table('projectMember')
  .columns({
    projectId: string(),
    userId: string(),
  })
  .primaryKey('projectId', 'userId');

// Relationships
const userRelationships = relationships(user, connect => ({
  ownedIssues: connect.many({
    sourceField: ['id'],
    destField: ['ownerId'],
    destSchema: issue,
  }),
  createdIssues: connect.many({
    sourceField: ['id'],
    destField: ['creatorId'],
    destSchema: issue,
  }),
  viewedIssues: connect.many(
    {
      sourceField: ['id'],
      destField: ['userId'],
      destSchema: viewState,
    },
    {
      sourceField: ['issueId'],
      destField: ['id'],
      destSchema: issue,
    },
  ),
  projects: connect.many(
    {
      sourceField: ['id'],
      destField: ['userId'],
      destSchema: projectMember,
    },
    {
      sourceField: ['projectId'],
      destField: ['id'],
      destSchema: project,
    },
  ),
}));

const issueRelationships = relationships(issue, connect => ({
  owner: connect.many({
    sourceField: ['ownerId'],
    destField: ['id'],
    destSchema: user,
  }),
  creator: connect.many({
    sourceField: ['creatorId'],
    destField: ['id'],
    destSchema: user,
  }),
  comments: connect.many({
    sourceField: ['id'],
    destField: ['issueId'],
    destSchema: comment,
  }),
  labels: connect.many(
    {
      sourceField: ['id'],
      destField: ['issueId'],
      destSchema: issueLabel,
    },
    {
      sourceField: ['labelId'],
      destField: ['id'],
      destSchema: label,
    },
  ),
  project: connect.many({
    sourceField: ['projectId'],
    destField: ['id'],
    destSchema: project,
  }),
  viewState: connect.many({
    sourceField: ['id'],
    destField: ['issueId'],
    destSchema: viewState,
  }),
}));

const commentRelationships = relationships(comment, connect => ({
  issue: connect.many({
    sourceField: ['issueId'],
    destField: ['id'],
    destSchema: issue,
  }),
  user: connect.many({
    sourceField: ['authorId'],
    destField: ['id'],
    destSchema: user,
  }),
}));

const issueLabelRelationships = relationships(issueLabel, connect => ({
  issue: connect.many({
    sourceField: ['issueId'],
    destField: ['id'],
    destSchema: issue,
  }),
  label: connect.many({
    sourceField: ['labelId'],
    destField: ['id'],
    destSchema: label,
  }),
}));

const viewStateRelationships = relationships(viewState, connect => ({
  user: connect.many({
    sourceField: ['userId'],
    destField: ['id'],
    destSchema: user,
  }),
  issue: connect.many({
    sourceField: ['issueId'],
    destField: ['id'],
    destSchema: issue,
  }),
}));

const projectRelationships = relationships(project, connect => ({
  issues: connect.many({
    sourceField: ['id'],
    destField: ['projectId'],
    destSchema: issue,
  }),
  members: connect.many(
    {
      sourceField: ['id'],
      destField: ['projectId'],
      destSchema: projectMember,
    },
    {
      sourceField: ['userId'],
      destField: ['id'],
      destSchema: user,
    },
  ),
}));

const projectMemberRelationships = relationships(projectMember, connect => ({
  project: connect.many({
    sourceField: ['projectId'],
    destField: ['id'],
    destSchema: project,
  }),
  user: connect.many({
    sourceField: ['userId'],
    destField: ['id'],
    destSchema: user,
  }),
}));

type AuthData = {
  sub: string;
  role: string;
  properties?: {
    role: string;
  };
};

const schema = createSchema({
  tables: [
    user,
    issue,
    comment,
    issueLabel,
    label,
    viewState,
    project,
    projectMember,
  ],
  relationships: [
    userRelationships,
    issueRelationships,
    commentRelationships,
    issueLabelRelationships,
    viewStateRelationships,
    projectRelationships,
    projectMemberRelationships,
  ],
});

type Schema = typeof schema;

const permissions = must(
  await definePermissions<AuthData, Schema>(schema, () => {
    const isCommentCreator = (
      authData: AuthData,
      {cmp}: ExpressionBuilder<'comment', Schema>,
    ) => cmp('authorId', '=', authData.sub);
    const isViewStateOwner = (
      authData: AuthData,
      {cmp}: ExpressionBuilder<'viewState', Schema>,
    ) => cmp('userId', '=', authData.sub);

    const canWriteIssueLabelIfProjectMember = (
      authData: AuthData,
      {exists}: ExpressionBuilder<'issueLabel', Schema>,
    ) =>
      exists('issue', q =>
        q.whereExists('project', q =>
          q.whereExists('members', q => q.where('id', '=', authData.sub)),
        ),
      );
    const canWriteIssueLabelIfIssueCreator = (
      authData: AuthData,
      {exists}: ExpressionBuilder<'issueLabel', Schema>,
    ) => exists('issue', q => q.where('creatorId', '=', authData.sub));
    const canWriteIssueLabelIfIssueOwner = (
      authData: AuthData,
      {exists}: ExpressionBuilder<'issueLabel', Schema>,
    ) => exists('issue', q => q.where('ownerId', '=', authData.sub));

    const canSeeIssue = (
      authData: AuthData,
      eb: ExpressionBuilder<'issue', Schema>,
    ) =>
      eb.or(
        isAdmin(authData, eb),
        isMemberOfProject(authData, eb),
        isIssueOwner(authData, eb),
        isIssueCreator(authData, eb),
        isAdminThroughNestedData(authData, eb),
      );

    const canSeeComment = (
      authData: AuthData,
      {exists}: ExpressionBuilder<'comment', Schema>,
    ) => exists('issue', q => q.where(eb => canSeeIssue(authData, eb)));

    const isAdmin = (
      authData: AuthData,
      {cmpLit}: ExpressionBuilder<string, ZeroSchema>,
    ) => cmpLit(authData.role, '=', 'admin');
    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    type TODO = any;
    const isAdminThroughNestedData = (
      authData: AuthData,
      {cmpLit}: ExpressionBuilder<string, ZeroSchema>,
      // TODO: proxy should return parameter references instead....
    ) => cmpLit(authData.properties?.role as TODO, 'IS', 'admin');

    const isMemberOfProject = (
      authData: AuthData,
      {exists}: ExpressionBuilder<'issue', Schema>,
    ) =>
      exists('project', q =>
        q.whereExists('members', q => q.where('id', '=', authData.sub)),
      );

    const isIssueOwner = (
      authData: AuthData,
      {cmp}: ExpressionBuilder<'issue', Schema>,
    ) => cmp('ownerId', '=', authData.sub);

    const isIssueCreator = (
      authData: AuthData,
      {cmp}: ExpressionBuilder<'issue', Schema>,
    ) => cmp('creatorId', '=', authData.sub);

    return {
      user: {
        row: {
          select: ANYONE_CAN,
        },
      },
      issue: {
        row: {
          insert: [
            (authData: AuthData, eb: ExpressionBuilder<'issue', Schema>) =>
              eb.and(
                isIssueCreator(authData, eb),
                eb.or(isAdmin(authData, eb), isMemberOfProject(authData, eb)),
              ),
          ],
          update: {
            preMutation: [
              isAdmin,
              isIssueCreator,
              isIssueOwner,
              isMemberOfProject,
            ],
            // TODO (mlaw): how can we ensure the creatorId is not changed?
            // We need to pass the OLD row to the postMutation rule.
            postMutation: ANYONE_CAN,
          },
          select: [canSeeIssue],
        },
      },
      comment: {
        row: {
          insert: [
            (authData: AuthData, eb: ExpressionBuilder<'comment', Schema>) =>
              eb.and(
                isCommentCreator(authData, eb),
                canSeeComment(authData, eb),
              ),
          ],
          update: {
            preMutation: [isAdmin, isCommentCreator],
            // TODO (mlaw): ensure that the authorId is not changed
            postMutation: ANYONE_CAN,
          },
          delete: [isAdmin, isCommentCreator],
          select: [canSeeComment],
        },
      },
      issueLabel: {
        row: {
          insert: [
            isAdmin,
            canWriteIssueLabelIfProjectMember,
            canWriteIssueLabelIfIssueCreator,
            canWriteIssueLabelIfIssueOwner,
          ],
          delete: [
            isAdmin,
            canWriteIssueLabelIfProjectMember,
            canWriteIssueLabelIfIssueCreator,
            canWriteIssueLabelIfIssueOwner,
          ],
        },
      },
      project: ANYONE_CAN_DO_ANYTHING,
      projectMember: ANYONE_CAN_DO_ANYTHING,
      viewState: {
        row: {
          select: ANYONE_CAN,
          insert: [isViewStateOwner],
          update: {
            preMutation: [isViewStateOwner],
            postMutation: [isViewStateOwner],
          },
          delete: [isViewStateOwner],
        },
      },
    };
  }),
);

let queryDelegate: QueryDelegate;
let replica: Database;
let writeAuthzStorage: DatabaseStorage;
function toDbType(type: ValueType) {
  switch (type) {
    case 'string':
      return 'TEXT';
    case 'number':
      return 'REAL';
    case 'boolean':
      return 'BOOLEAN';
    default:
      throw new Error(`Unknown type ${type}`);
  }
}
let writeAuthorizer: WriteAuthorizerImpl;

class ReadAuthorizerTestQueryDelegate extends QueryDelegateBase {
  readonly defaultQueryComplete = true;

  readonly #sources = new Map<string, Source>();
  readonly #replica: Database;
  readonly #lc: LogContext;

  constructor(replica: Database, lc: LogContext) {
    super();
    this.#replica = replica;
    this.#lc = lc;
  }

  override getSource(name: string): Source {
    let source = this.#sources.get(name);
    if (source) {
      return source;
    }
    const tableSchema = schema.tables[name as keyof Schema['tables']];
    assert(tableSchema, `Table schema not found for ${name}`);

    // create the SQLite table
    this.#replica.exec(`
      CREATE TABLE "${name}" (
        ${Object.entries(tableSchema.columns)
          .map(([name, c]) => `"${name}" ${toDbType(c.type)}`)
          .join(', ')},
        PRIMARY KEY (${tableSchema.primaryKey.map(k => `"${k}"`).join(', ')})
      )`);

    source = new TableSource(
      this.#lc,
      testLogConfig,
      this.#replica,
      name,
      tableSchema.columns,
      tableSchema.primaryKey,
    );

    this.#sources.set(name, source);
    return source;
  }
}

beforeEach(() => {
  replica = new Database(lc, ':memory:');
  replica.exec(`
    CREATE TABLE "app.permissions" (permissions JSON, hash TEXT);
  `);
  const permsJSON = JSON.stringify(permissions);
  replica
    .prepare(`INSERT INTO "app.permissions" (permissions, hash) VALUES (?, ?)`)
    .run(permsJSON, h128(permsJSON).toString(16));

  queryDelegate = new ReadAuthorizerTestQueryDelegate(replica, lc);

  for (const table of Object.values(schema.tables)) {
    // force the sqlite tables to be created by getting all the sources
    must(queryDelegate.getSource(table.name));
  }

  const storageDb = new Database(lc, ':memory:');
  storageDb.prepare(CREATE_STORAGE_TABLE).run();
  writeAuthzStorage = new DatabaseStorage(storageDb);

  writeAuthorizer = new WriteAuthorizerImpl(
    lc,
    zeroConfig,
    replica,
    'app',
    'cg',
    writeAuthzStorage,
  );
});
const lc = createSilentLogContext();

test('cannot create an issue with the wrong creatorId, even if admin', async () => {
  const ops = [
    {
      op: 'insert',
      tableName: 'issue',
      primaryKey: ['id'],
      value: {
        id: '004',
        title: 'Iss 4',
        description: '',
        closed: false,
        ownerId: '001',
        creatorId: '002',
        projectId: '001',
      },
    },
  ] as InsertOp[];
  let authData: AuthData = {
    sub: '001',
    role: 'admin',
  };
  expect(
    (await writeAuthorizer.canPreMutation(authData, ops)) &&
      (await writeAuthorizer.canPostMutation(authData, ops)),
  ).toBe(false);

  authData = {
    sub: '002',
    role: 'admin',
  };
  expect(
    (await writeAuthorizer.canPreMutation(authData, ops)) &&
      (await writeAuthorizer.canPostMutation(authData, ops)),
  ).toBe(true);
});

function addUser(user: Row<Schema['tables']['user']>) {
  const userSource = must(queryDelegate.getSource('user'));
  consume(
    userSource.push({
      type: 'add',
      row: user,
    }),
  );
}

function addProject(project: Row<Schema['tables']['project']>) {
  const projectSource = must(queryDelegate.getSource('project'));
  consume(
    projectSource.push({
      type: 'add',
      row: project,
    }),
  );
}

function addProjectMember(
  projectMember: Row<Schema['tables']['projectMember']>,
) {
  const projectMemberSource = must(queryDelegate.getSource('projectMember'));
  consume(
    projectMemberSource.push({
      type: 'add',
      row: projectMember,
    }),
  );
}

function addIssue(issue: Row<Schema['tables']['issue']>) {
  const issueSource = must(queryDelegate.getSource('issue'));
  consume(
    issueSource.push({
      type: 'add',
      row: issue,
    }),
  );
}

function addComment(comment: Row<Schema['tables']['comment']>) {
  const commentSource = must(queryDelegate.getSource('comment'));
  consume(
    commentSource.push({
      type: 'add',
      row: comment,
    }),
  );
}

function addLabel(label: Row<Schema['tables']['label']>) {
  const labelSource = must(queryDelegate.getSource('label'));
  consume(
    labelSource.push({
      type: 'add',
      row: label,
    }),
  );
}

function addIssueLabel(issueLabel: Row<Schema['tables']['issueLabel']>) {
  const issueLabelSource = must(queryDelegate.getSource('issueLabel'));
  consume(
    issueLabelSource.push({
      type: 'add',
      row: issueLabel,
    }),
  );
}

function addViewState(viewState: Row<Schema['tables']['viewState']>) {
  const viewStateSource = must(queryDelegate.getSource('viewState'));
  consume(
    viewStateSource.push({
      type: 'add',
      row: viewState,
    }),
  );
}

test('cannot create an issue unless you are a project member', async () => {
  addUser({id: '001', name: 'Alice', role: 'user'});
  addUser({id: '002', name: 'Bob', role: 'user'});
  // project 1
  addProject({id: '001', name: 'Project 1'});
  addProjectMember({projectId: '001', userId: '001'});
  // project 2
  addProject({id: '002', name: 'Project 2'});
  addProjectMember({projectId: '002', userId: '002'});

  const op: InsertOp = {
    op: 'insert',
    tableName: 'issue',
    primaryKey: ['id'],
    value: {
      id: '004',
      title: 'Iss 4',
      description: '',
      closed: false,
      ownerId: '001',
      creatorId: '001',
      projectId: '001',
    },
  };
  let authData = {sub: '001', role: 'user'};
  // user 1 is a member of project 1 and creator of the issue
  expect(
    (await writeAuthorizer.canPreMutation(authData, [op])) &&
      (await writeAuthorizer.canPostMutation(authData, [op])),
  ).toBe(true);

  // user 2 is not a member of project 1
  const op2 = {
    ...op,
    value: {...op.value, creatorId: '002'},
  };
  authData = {sub: '002', role: 'user'};
  expect(
    (await writeAuthorizer.canPreMutation(authData, [op2])) &&
      (await writeAuthorizer.canPostMutation(authData, [op2])),
  ).toBe(false);

  // user 2 is a member of project 2
  const op3 = {
    ...op2,
    value: {...op2.value, projectId: '002'},
  };
  expect(
    (await writeAuthorizer.canPreMutation(authData, [op3])) &&
      (await writeAuthorizer.canPostMutation(authData, [op3])),
  ).toBe(true);
});

describe('issue permissions', () => {
  beforeEach(() => {
    addUser({id: '001', name: 'Alice', role: 'user'});
    addUser({id: '002', name: 'Bob', role: 'user'});
    addUser({id: '003', name: 'Charlie', role: 'user'});
    addUser({id: '011', name: 'David', role: 'user'});
    addUser({id: '012', name: 'Eve', role: 'user'});

    addProject({id: '001', name: 'Project 1'});
    addProjectMember({projectId: '001', userId: '001'});
    addProjectMember({projectId: '001', userId: '011'});

    addProject({id: '002', name: 'Project 2'});
    addProjectMember({projectId: '002', userId: '012'});

    addIssue({
      id: '001',
      title: 'Project member test',
      description: 'This is the first issue',
      closed: false,
      ownerId: '003',
      creatorId: '003',
      projectId: '001',
    });

    addIssue({
      id: '002',
      title: 'Creator test',
      description: '',
      closed: false,
      ownerId: '003',
      creatorId: '001',
      projectId: '002',
    });

    addIssue({
      id: '003',
      title: 'Owner test',
      description: '',
      closed: false,
      ownerId: '001',
      creatorId: '003',
      projectId: '002',
    });
  });

  test('update as project member', async () => {
    const op: UpdateOp = {
      op: 'update',
      tableName: 'issue',
      primaryKey: ['id'],
      value: {id: '001', closed: true},
    };
    let authData = {sub: '001', role: 'user'};
    // user 1 is a member of project 1 so they can update the issue
    expect(
      (await writeAuthorizer.canPreMutation(authData, [op])) &&
        (await writeAuthorizer.canPostMutation(authData, [op])),
    ).toBe(true);

    // user 2 is not a project member (or owner or creator) of issue 1 so they cannot update the issue
    authData = {sub: '002', role: 'user'};
    expect(
      (await writeAuthorizer.canPreMutation(authData, [op])) &&
        (await writeAuthorizer.canPostMutation(authData, [op])),
    ).toBe(false);
  });

  test('update as creator', async () => {
    const op: UpdateOp = {
      op: 'update',
      tableName: 'issue',
      primaryKey: ['id'],
      value: {id: '002', closed: true},
    };

    let authData = {sub: '001', role: 'user'};
    // user 1 is the creator of issue 2 so they can update the issue
    expect(
      (await writeAuthorizer.canPreMutation(authData, [op])) &&
        (await writeAuthorizer.canPostMutation(authData, [op])),
    ).toBe(true);

    // user 2 is not a creator (or owner or project member) of issue 2 so they cannot update the issue
    authData = {sub: '002', role: 'user'};
    expect(
      (await writeAuthorizer.canPreMutation(authData, [op])) &&
        (await writeAuthorizer.canPostMutation(authData, [op])),
    ).toBe(false);
  });

  test('update as owner', async () => {
    const op: UpdateOp = {
      op: 'update',
      tableName: 'issue',
      primaryKey: ['id'],
      value: {id: '003', closed: true},
    };

    let authData = {sub: '001', role: 'user'};
    // user 1 is the owner of issue 3 so they can update the issue
    expect(
      (await writeAuthorizer.canPreMutation(authData, [op])) &&
        (await writeAuthorizer.canPostMutation(authData, [op])),
    ).toBe(true);

    // user 2 is not a owner (or creator or project member) of issue 3 so they cannot update the issue
    authData = {sub: '002', role: 'user'};
    expect(
      (await writeAuthorizer.canPreMutation(authData, [op])) &&
        (await writeAuthorizer.canPostMutation(authData, [op])),
    ).toBe(false);
  });

  test('update as admin', async () => {
    const op: UpdateOp = {
      op: 'update',
      tableName: 'issue',
      primaryKey: ['id'],
      value: {id: '003', closed: true},
    };

    const authData = {sub: '005', role: 'admin'};
    expect(
      (await writeAuthorizer.canPreMutation(authData, [op])) &&
        (await writeAuthorizer.canPostMutation(authData, [op])),
    ).toBe(true);
  });

  test('view as admin', () => {
    // Admin can see all of the issues
    expect(
      runReadQueryWithPermissions(
        {sub: '005', role: 'admin'},
        newQuery(schema, 'issue'),
        queryDelegate,
      ).map(r => r.row.id),
    ).toEqual(['001', '002', '003']);
  });

  test('view as project member, creator or owner', () => {
    // user 1 is project member for issue 1, creator of issue 2 and owner of issue 3
    expect(
      runReadQueryWithPermissions(
        {sub: '001', role: 'user'},
        newQuery(schema, 'issue'),
        queryDelegate,
      ).map(r => r.row.id),
    ).toEqual(['001', '002', '003']);

    // user 2 is not a project member, creator or owner of any issues
    expect(
      runReadQueryWithPermissions(
        {sub: '002', role: 'user'},
        newQuery(schema, 'issue'),
        queryDelegate,
      ).map(r => r.row.id),
    ).toEqual([]);

    // user 3 is creator / owner of all issues
    expect(
      runReadQueryWithPermissions(
        {sub: '003', role: 'user'},
        newQuery(schema, 'issue'),
        queryDelegate,
      ).map(r => r.row.id),
    ).toEqual(['001', '002', '003']);

    // user 11 is only a member of project 1
    expect(
      runReadQueryWithPermissions(
        {sub: '011', role: 'user'},
        newQuery(schema, 'issue'),
        queryDelegate,
      ).map(r => r.row.id),
    ).toEqual(['001']);

    // user 12 is only a member of project 2
    expect(
      runReadQueryWithPermissions(
        {sub: '012', role: 'user'},
        newQuery(schema, 'issue'),
        queryDelegate,
      ).map(r => r.row.id),
    ).toEqual(['002', '003']);
  });

  test('cannot delete an issue', async () => {
    const op: DeleteOp = {
      op: 'delete',
      tableName: 'issue',
      primaryKey: ['id'],
      value: {id: '003'},
    };

    for (const sub of ['001', '002', '003']) {
      const authData = {sub, role: 'user'};
      expect(
        (await writeAuthorizer.canPreMutation(authData, [op])) &&
          (await writeAuthorizer.canPostMutation(authData, [op])),
      ).toBe(false);
    }

    const authData = {sub: '005', role: 'admin'};
    expect(
      (await writeAuthorizer.canPreMutation(authData, [op])) &&
        (await writeAuthorizer.canPostMutation(authData, [op])),
    ).toBe(false);
  });
});

function runReadQueryWithPermissions(
  authData: AuthData,
  query: QueryBuilder<string, ZeroSchema>,
  queryDelegate: QueryDelegate,
) {
  const updatedAst = bindStaticParameters(
    transformQuery(
      new LogContext('debug'),
      asQueryInternals(query).ast,
      permissions,
      authData,
    ),
    {
      authData,
      preMutationRow: undefined,
    },
  );
  const pipeline = buildPipeline(updatedAst, queryDelegate, 'query-id');
  const out = new Catch(pipeline);
  return out.fetch({}).filter(n => n !== 'yield');
}

describe('comment & issueLabel permissions', () => {
  beforeEach(() => {
    // can see issue 1 via project membership
    addUser({id: '001', name: 'Alice', role: 'user'});
    // can see issue 1 by being its creator
    addUser({id: '002', name: 'Bob', role: 'user'});
    // can see issue 1 by being its owner
    addUser({id: '003', name: 'Charlie', role: 'user'});
    // cannot see any issues
    addUser({id: '004', name: 'David', role: 'user'});
    // can see issue 1 by being admin
    addUser({id: '005', name: 'David', role: 'admin'});

    addProject({id: '001', name: 'Project 1'});
    addProjectMember({projectId: '001', userId: '001'});

    addIssue({
      id: '001',
      title: 'Issue 1',
      description: 'This is the first issue',
      closed: false,
      ownerId: '003',
      creatorId: '002',
      projectId: '001',
    });

    addComment({
      id: '001',
      issueId: '001',
      authorId: '001',
      text: 'Comment 1',
    });

    addComment({
      id: '002',
      issueId: '001',
      authorId: '002',
      text: 'Comment 2',
    });

    addLabel({
      id: '001',
      name: 'Label 1',
    });

    addIssueLabel({
      issueId: '001',
      labelId: '001',
    });
  });

  test('cannot set authorId to another user for a comment on insert', async () => {
    let op: InsertOp = {
      op: 'insert',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {
        id: '011',
        issueId: '001',
        authorId: '001',
        text: 'This is a comment',
      },
    };
    let authData = {sub: '002', role: 'user'};

    // sub and author mismatch
    expect(
      (await writeAuthorizer.canPreMutation(authData, [op])) &&
        (await writeAuthorizer.canPostMutation(authData, [op])),
    ).toBe(false);

    // sub and author match
    // we use `sub 002` to ensure that the false above wasn't due to some other reason besides
    // sub and author mismatch.
    op = {
      op: 'insert',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {
        id: '011',
        issueId: '001',
        authorId: '002',
        text: 'This is a comment',
      },
    };
    authData = {sub: '002', role: 'user'};
    expect(
      (await writeAuthorizer.canPreMutation(authData, [op])) &&
        (await writeAuthorizer.canPostMutation(authData, [op])),
    ).toBe(true);
  });

  test('cannot create a comment for an issue you cannot see', async () => {
    const op: InsertOp = {
      op: 'insert',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {
        id: '011',
        issueId: '001',
        authorId: '004',
        text: 'This is a comment',
      },
    };

    let authData = {sub: '004', role: 'user'};
    // user 4 cannot see the issue so this fails
    expect(
      (await writeAuthorizer.canPreMutation(authData, [op])) &&
        (await writeAuthorizer.canPostMutation(authData, [op])),
    ).toBe(false);

    // upgrading user 4 to admin should allow them to see the issue and write the comment
    authData = {sub: '004', role: 'admin'};
    expect(
      (await writeAuthorizer.canPreMutation(authData, [op])) &&
        (await writeAuthorizer.canPostMutation(authData, [op])),
    ).toBe(true);
  });

  test('cannot update a comment unless you created the comment or are the admin', async () => {
    let op: UpdateOp = {
      op: 'update',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: '001', text: 'updated comment'},
    };
    // user 2 did not create comment 1
    const authData = {sub: '002', role: 'user'};
    expect(
      (await writeAuthorizer.canPreMutation(authData, [op])) &&
        (await writeAuthorizer.canPostMutation(authData, [op])),
    ).toBe(false);

    // user 2 did create comment 2
    op = {
      op: 'update',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: '002', text: 'updated comment'},
    };
    expect(
      (await writeAuthorizer.canPreMutation(authData, [op])) &&
        (await writeAuthorizer.canPostMutation(authData, [op])),
    ).toBe(true);
  });

  test('cannot delete a comment unless you are the admin or the author of the comment', async () => {
    let op: DeleteOp = {
      op: 'delete',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: '001'},
    };
    let authData = {sub: '002', role: 'user'};
    // user 2 did not create comment 1
    expect(
      (await writeAuthorizer.canPreMutation(authData, [op])) &&
        (await writeAuthorizer.canPostMutation(authData, [op])),
    ).toBe(false);

    // user 2 did create comment 2
    op = {
      op: 'delete',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: '002'},
    };
    expect(
      (await writeAuthorizer.canPreMutation(authData, [op])) &&
        (await writeAuthorizer.canPostMutation(authData, [op])),
    ).toBe(true);

    // user 5 is an admin so they can delete any comment
    authData = {sub: '005', role: 'admin'};
    op = {
      op: 'delete',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: '001'},
    };
    expect(
      (await writeAuthorizer.canPreMutation(authData, [op])) &&
        (await writeAuthorizer.canPostMutation(authData, [op])),
    ).toBe(true);
  });

  test('cannot see a comment unless you can see the issue', () => {
    // users 1, 2 and 3 and 5 can see all comments because they can see the issue
    // user 4 cannot see any comments because they cannot see any issues
    for (const sub of ['001', '002', '003', '005']) {
      expect(
        runReadQueryWithPermissions(
          {sub, role: sub === '005' ? 'admin' : 'user'},
          newQuery(schema, 'comment'),
          queryDelegate,
        ).map(r => r.row.id),
      ).toEqual(['001', '002']);
    }

    expect(
      runReadQueryWithPermissions(
        {sub: '004', role: 'user'},
        newQuery(schema, 'comment'),
        queryDelegate,
      ).map(r => r.row.id),
    ).toEqual([]);
  });

  test('cannot insert an issueLabel if not admin/project-member/issue-creator/issue-owner', async () => {
    for (const opType of ['insert', 'delete'] as const) {
      const op: InsertOp | UpdateOp | DeleteOp = {
        op: opType,
        tableName: 'issueLabel',
        primaryKey: ['issueId', 'labelId'],
        value: {labelId: opType === 'insert' ? '002' : '001', issueId: '001'},
      };

      let authData = {sub: '004', role: 'user'};
      // user 4 cannot see the issue so this fails
      expect(
        (await writeAuthorizer.canPreMutation(authData, [op])) &&
          (await writeAuthorizer.canPostMutation(authData, [op])),
      ).toBe(false);

      // upgrading user 4 to admin should allow them to see the issue and write the issueLabel
      authData = {sub: '004', role: 'admin'};
      expect(
        (await writeAuthorizer.canPreMutation(authData, [op])) &&
          (await writeAuthorizer.canPostMutation(authData, [op])),
      ).toBe(true);

      for (const sub of ['001', '002', '003']) {
        authData = {sub, role: 'user'};
        expect(
          (await writeAuthorizer.canPreMutation(authData, [op])) &&
            (await writeAuthorizer.canPostMutation(authData, [op])),
        ).toBe(true);
      }
    }
  });
});

test('can only insert a viewState if you are the owner', async () => {
  addViewState({userId: '001', issueId: '001', lastRead: 1234});
  for (const opType of ['insert', 'update', 'delete'] as const) {
    const op: InsertOp | UpdateOp | DeleteOp = {
      op: opType,
      tableName: 'viewState',
      primaryKey: ['issueId', 'userId'],
      value: {
        issueId: opType === 'insert' ? '002' : '001',
        userId: '001',
        lastRead: 1234,
      },
    };

    let authData = {sub: '001', role: 'user'};
    // user 1 can insert/update/delete a viewState for user 1
    expect(
      (await writeAuthorizer.canPreMutation(authData, [op])) &&
        (await writeAuthorizer.canPostMutation(authData, [op])),
    ).toBe(true);

    // user 2 cannot insert/update/delete a viewState for user 1
    authData = {sub: '002', role: 'user'};
    expect(
      (await writeAuthorizer.canPreMutation(authData, [op])) &&
        (await writeAuthorizer.canPostMutation(authData, [op])),
    ).toBe(false);
  }
});

describe('read permissions against nested paths', () => {
  beforeEach(() => {
    addUser({id: 'owner-creator', name: 'Alice', role: 'user'});
    addUser({id: 'project-member', name: 'Bob', role: 'user'});
    addUser({id: 'not-project-member', name: 'Charlie', role: 'user'});

    addIssue({
      id: '001',
      title: 'Issue 1',
      description: 'This is the first issue',
      closed: false,
      ownerId: 'owner-creator',
      creatorId: 'owner-creator',
      projectId: '001',
    });
    addIssue({
      id: '002',
      title: 'Issue 2',
      description: 'This is the second issue',
      closed: false,
      ownerId: 'owner-creator',
      creatorId: 'owner-creator',
      projectId: '001',
    });

    addProject({id: '001', name: 'Project 1'});
    addProjectMember({projectId: '001', userId: 'project-member'});

    addViewState({
      userId: 'owner-creator',
      issueId: '001',
      lastRead: 1234,
    });
    addViewState({
      userId: 'owner-creator',
      issueId: '002',
      lastRead: 1234,
    });
    addViewState({
      userId: 'project-member',
      issueId: '001',
      lastRead: 1234,
    });
    addViewState({
      userId: 'project-member',
      issueId: '002',
      lastRead: 1234,
    });
    addViewState({
      userId: 'not-project-member',
      issueId: '001',
      lastRead: 1234,
    });
    addViewState({
      userId: 'not-project-member',
      issueId: '002',
      lastRead: 1234,
    });

    addComment({
      id: '001',
      issueId: '001',
      authorId: 'owner-creator',
      text: 'Comment 1',
    });
    addComment({
      id: '002',
      issueId: '001',
      authorId: 'project-member',
      text: 'Comment 2',
    });
    addComment({
      id: '003',
      issueId: '001',
      authorId: 'not-project-member',
      text: 'Comment 3',
    });
    addComment({
      id: '004',
      issueId: '002',
      authorId: 'owner-creator',
      text: 'Comment 1',
    });
    addComment({
      id: '005',
      issueId: '002',
      authorId: 'project-member',
      text: 'Comment 2',
    });
    addComment({
      id: '006',
      issueId: '002',
      authorId: 'not-project-member',
      text: 'Comment 3',
    });

    addLabel({
      id: '001',
      name: 'Label 1',
    });
    addIssueLabel({
      issueId: '001',
      labelId: '001',
    });
    addIssueLabel({
      issueId: '002',
      labelId: '001',
    });
  });

  test.each([
    {
      name: 'User can view everything they are attached to through owner/creator relationships',
      sub: 'owner-creator',
      query: newQuery(schema, 'user')
        .where('id', '=', 'owner-creator')
        .related('createdIssues', q => q.related('comments', q => q.limit(1)))
        .related('ownedIssues', q => q.related('comments', q => q.limit(1))),
      expected: [
        {
          id: 'owner-creator',
          createdIssues: [
            {
              id: '001',
              comments: [
                {
                  id: '001',
                },
              ],
            },
            {
              id: '002',
              comments: [
                {
                  id: '004',
                },
              ],
            },
          ],
          ownedIssues: [
            {
              id: '001',
              comments: [
                {
                  id: '001',
                },
              ],
            },
            {
              id: '002',
              comments: [
                {
                  id: '004',
                },
              ],
            },
          ],
        },
      ],
    },
    {
      name: 'User cannot see previously viewed issues if they were moved out of the project and are not the owner/creator',
      sub: 'not-project-member',
      query: newQuery(schema, 'user')
        .where('id', '=', 'not-project-member')
        .related('viewedIssues', q => q.related('comments')),
      expected: [
        {
          id: 'not-project-member',
          viewedIssues: [
            {
              viewedIssues: [],
            },
            {
              viewedIssues: [],
            },
          ],
        },
      ],
    },
    {
      name: 'User can see previously viewed issues (even if they are not in the project) if they are the owner/creator',
      sub: 'owner-creator',
      query: newQuery(schema, 'user')
        .where('id', 'owner-creator')
        .related('viewedIssues', q => q.related('comments', q => q.limit(2))),
      expected: [
        {
          id: 'owner-creator',
          viewedIssues: [
            {
              viewedIssues: [
                {
                  id: '001',
                  comments: [
                    {
                      id: '001',
                    },
                    {
                      id: '002',
                    },
                  ],
                },
              ],
            },
            {
              viewedIssues: [
                {
                  id: '002',
                  comments: [
                    {
                      id: '004',
                    },
                    {
                      id: '005',
                    },
                  ],
                },
              ],
            },
          ],
        },
      ],
    },
    {
      name: 'User can see everything they are attached to through project membership',
      sub: 'project-member',
      query: newQuery(schema, 'user').related('projects', q =>
        q.related('issues', q => q.related('comments')),
      ),
      expected: [
        {
          id: 'not-project-member',
          projects: [],
        },
        {
          id: 'owner-creator',
          projects: [],
        },
        {
          id: 'project-member',
          projects: [
            {
              projects: [
                {
                  id: '001',
                  issues: [
                    {
                      id: '001',
                      comments: [
                        {
                          id: '001',
                        },
                        {
                          id: '002',
                        },
                        {
                          id: '003',
                        },
                      ],
                    },
                    {
                      id: '002',
                      comments: [
                        {
                          id: '004',
                        },
                        {
                          id: '005',
                        },
                        {
                          id: '006',
                        },
                      ],
                    },
                  ],
                },
              ],
            },
          ],
        },
      ],
    },
  ])('$name', ({sub, query, expected}) => {
    const actual = runReadQueryWithPermissions(
      {
        sub,
        role: sub === 'admin' ? 'admin' : 'user',
      },
      query,
      queryDelegate,
    );
    expect(toIdsOnly(actual)).toEqual(expected);
  });
});

describe('read permissions against nested paths', () => {
  beforeEach(() => {
    addUser({id: 'owner-creator', name: 'Alice', role: 'user'});
    addUser({id: 'project-member', name: 'Bob', role: 'user'});
    addUser({id: 'not-project-member', name: 'Charlie', role: 'user'});

    addIssue({
      id: '001',
      title: 'Issue 1',
      description: 'This is the first issue',
      closed: false,
      ownerId: 'owner-creator',
      creatorId: 'owner-creator',
      projectId: '001',
    });
  });

  test('nested property access', () => {
    let actual = runReadQueryWithPermissions(
      {sub: 'dne', role: '', properties: {role: 'admin'}},
      newQuery(schema, 'issue'),
      queryDelegate,
    );
    expect(toIdsOnly(actual)).toEqual([
      {
        id: '001',
      },
    ]);

    actual = runReadQueryWithPermissions(
      {sub: 'dne', role: ''},
      newQuery(schema, 'issue'),
      queryDelegate,
    );
    expect(toIdsOnly(actual)).toEqual([]);
  });
});

// maps over nodes, drops all information from `row` except the id
// oxlint-disable-next-line @typescript-eslint/no-explicit-any
function toIdsOnly(nodes: CaughtNode[]): any[] {
  return nodes
    .filter(n => n !== 'yield')
    .map(node => ({
      id: node.row.id,
      ...Object.fromEntries(
        Object.entries(node.relationships)
          .filter(([k]) => !k.startsWith('zsubq_'))
          .map(([k, v]) => [k, toIdsOnly(Array.isArray(v) ? v : [...v])]),
      ),
    }));
}

// TODO (mlaw): test that `exists` does not provide an oracle
