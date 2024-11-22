/* eslint-disable @typescript-eslint/naming-convention */
/* eslint-disable arrow-body-style */
import {beforeEach, describe, expect, test} from 'vitest';
import {
  completedAstSymbol,
  newQuery,
  QueryImpl,
  type QueryDelegate,
} from '../../../zql/src/query/query-impl.js';
import {Database} from '../../../zqlite/src/db.js';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.js';
import type {Source} from '../../../zql/src/ivm/source.js';
import type {
  TableSchema,
  TableSchemaToRow,
  ValueType,
} from '../../../zero-schema/src/table-schema.js';
import {TableSource} from '../../../zqlite/src/table-source.js';
import {MemoryStorage} from '../../../zql/src/ivm/memory-storage.js';
import {must} from '../../../shared/src/must.js';
import {defineAuthorization} from '../../../zero-schema/src/authorization.js';
import type {ExpressionBuilder} from '../../../zql/src/query/expression.js';
import {WriteAuthorizerImpl} from './write-authorizer.js';
import type {
  DeleteOp,
  InsertOp,
  UpdateOp,
} from '../../../zero-protocol/src/push.js';
import {assert} from '../../../shared/src/asserts.js';
import {transformQuery} from './read-authorizer.js';
import type {Query, QueryType} from '../../../zql/src/query/query.js';
import {Catch} from '../../../zql/src/ivm/catch.js';
import {
  bindStaticParameters,
  buildPipeline,
} from '../../../zql/src/builder/builder.js';
import type {Node} from '../../../zql/src/ivm/data.js';

const schema = {
  version: 1,
  tables: {
    user: {
      tableName: 'user',
      columns: {
        id: {type: 'string'},
        name: {type: 'string'},
        role: {type: 'string'},
      },
      primaryKey: ['id'],
      relationships: {
        ownedIssues: {
          dest: {
            field: 'ownerId',
            schema: () => schema.tables.issue,
          },
          source: 'id',
        },
        createdIssues: {
          dest: {
            field: 'creatorId',
            schema: () => schema.tables.issue,
          },
          source: 'id',
        },
        viewedIssues: {
          source: 'id',
          junction: {
            schema: () => schema.tables.viewState,
            destField: 'issueId',
            sourceField: 'userId',
          },
          dest: {
            field: 'id',
            schema: () => schema.tables.issue,
          },
        },
        projects: {
          source: 'id',
          junction: {
            schema: () => schema.tables.projectMember,
            destField: 'projectId',
            sourceField: 'userId',
          },
          dest: {
            field: 'id',
            schema: () => schema.tables.project,
          },
        },
      },
    },
    issue: {
      tableName: 'issue',
      columns: {
        id: {type: 'string'},
        title: {type: 'string'},
        description: {type: 'string'},
        closed: {type: 'boolean'},
        ownerId: {type: 'string'},
        creatorId: {type: 'string'},
        projectId: {type: 'string'},
      },
      primaryKey: ['id'],
      relationships: {
        owner: {
          dest: {
            field: 'id',
            schema: () => schema.tables.user,
          },
          source: 'ownerId',
        },
        creator: {
          dest: {
            field: 'id',
            schema: () => schema.tables.user,
          },
          source: 'creatorId',
        },
        comments: {
          dest: {
            field: 'issueId',
            schema: () => schema.tables.comment,
          },
          source: 'id',
        },
        labels: {
          junction: {
            schema: () => schema.tables.issueLabel,
            destField: 'labelId',
            sourceField: 'issueId',
          },
          dest: {
            field: 'id',
            schema: () => schema.tables.label,
          },
          source: 'id',
        },
        project: {
          dest: {
            field: 'id',
            schema: () => schema.tables.project,
          },
          source: 'projectId',
        },
        viewState: {
          dest: {
            field: 'issueId',
            schema: () => schema.tables.viewState,
          },
          source: 'id',
        },
      },
    },
    comment: {
      tableName: 'comment',
      columns: {
        id: {type: 'string'},
        issueId: {type: 'string'},
        authorId: {type: 'string'},
        text: {type: 'string'},
      },
      primaryKey: ['id'],
      relationships: {
        issue: {
          dest: {
            field: 'id',
            schema: () => schema.tables.issue,
          },
          source: 'issueId',
        },
        user: {
          dest: {
            field: 'id',
            schema: () => schema.tables.user,
          },
          source: 'authorId',
        },
      },
    },
    issueLabel: {
      tableName: 'issueLabel',
      columns: {
        issueId: {type: 'string'},
        labelId: {type: 'string'},
      },
      primaryKey: ['issueId', 'labelId'],
      relationships: {
        issue: {
          dest: {
            field: 'id',
            schema: () => schema.tables.issue,
          },
          source: 'issueId',
        },
        label: {
          dest: {
            field: 'id',
            schema: () => schema.tables.label,
          },
          source: 'labelId',
        },
      },
    },
    label: {
      tableName: 'label',
      columns: {
        id: {type: 'string'},
        name: {type: 'string'},
      },
      primaryKey: ['id'],
      relationships: {},
    },
    viewState: {
      tableName: 'viewState',
      columns: {
        userId: {type: 'string'},
        issueId: {type: 'string'},
        lastRead: {type: 'number'},
      },
      primaryKey: ['issueId', 'userId'],
      relationships: {
        user: {
          dest: {
            field: 'id',
            schema: () => schema.tables.user,
          },
          source: 'userId',
        },
        issue: {
          dest: {
            field: 'id',
            schema: () => schema.tables.issue,
          },
          source: 'issueId',
        },
      },
    },
    project: {
      tableName: 'project',
      columns: {
        id: {type: 'string'},
        name: {type: 'string'},
      },
      primaryKey: ['id'],
      relationships: {
        issues: {
          dest: {
            field: 'projectId',
            schema: () => schema.tables.issue,
          },
          source: 'id',
        },
        members: {
          junction: {
            schema: () => schema.tables.projectMember,
            destField: 'userId',
            sourceField: 'projectId',
          },
          dest: {
            field: 'id',
            schema: () => schema.tables.user,
          },
          source: 'id',
        },
      },
    },
    projectMember: {
      tableName: 'projectMember',
      columns: {
        projectId: {type: 'string'},
        userId: {type: 'string'},
      },
      primaryKey: ['projectId', 'userId'],
      relationships: {
        project: {
          dest: {
            field: 'id',
            schema: () => schema.tables.project,
          },
          source: 'projectId',
        },
        user: {
          dest: {
            field: 'id',
            schema: () => schema.tables.user,
          },
          source: 'userId',
        },
      },
    },
  },
} as const;

type AuthData = {
  sub: string;
  role: string;
};

// eslint-disable-next-line arrow-body-style
const permissions = must(
  await defineAuthorization<AuthData, typeof schema>(schema, () => {
    const isCommentCreator = (
      authData: AuthData,
      {cmp}: ExpressionBuilder<typeof schema.tables.comment>,
    ) => cmp('authorId', '=', authData.sub);
    const isViewStateOwner = (
      authData: AuthData,
      {cmp}: ExpressionBuilder<typeof schema.tables.viewState>,
    ) => cmp('userId', '=', authData.sub);

    const canWriteIssueLabelIfProjectMember = (
      authData: AuthData,
      {exists}: ExpressionBuilder<typeof schema.tables.issueLabel>,
    ) =>
      exists('issue', q =>
        q.whereExists('project', q =>
          q.whereExists('members', q => q.where('id', '=', authData.sub)),
        ),
      );
    const canWriteIssueLabelIfIssueCreator = (
      authData: AuthData,
      {exists}: ExpressionBuilder<typeof schema.tables.issueLabel>,
    ) => exists('issue', q => q.where('creatorId', '=', authData.sub));
    const canWriteIssueLabelIfIssueOwner = (
      authData: AuthData,
      {exists}: ExpressionBuilder<typeof schema.tables.issueLabel>,
    ) => exists('issue', q => q.where('ownerId', '=', authData.sub));

    const canSeeIssue = (
      authData: AuthData,
      eb: ExpressionBuilder<typeof schema.tables.issue>,
    ) =>
      eb.or(
        isAdmin(authData, eb),
        isMemberOfProject(authData, eb),
        isIssueOwner(authData, eb),
        isIssueCreator(authData, eb),
      );

    const canSeeComment = (
      authData: AuthData,
      {exists}: ExpressionBuilder<typeof schema.tables.comment>,
    ) => exists('issue', q => q.where(eb => canSeeIssue(authData, eb)));

    const isAdmin = (
      authData: AuthData,
      {cmpLit}: ExpressionBuilder<TableSchema>,
    ) => cmpLit(authData.role, '=', 'admin');

    const isMemberOfProject = (
      authData: AuthData,
      {exists}: ExpressionBuilder<typeof schema.tables.issue>,
    ) =>
      exists('project', q =>
        q.whereExists('members', q => q.where('id', '=', authData.sub)),
      );

    const isIssueOwner = (
      authData: AuthData,
      {cmp}: ExpressionBuilder<typeof schema.tables.issue>,
    ) => cmp('ownerId', '=', authData.sub);

    const isIssueCreator = (
      authData: AuthData,
      {cmp}: ExpressionBuilder<typeof schema.tables.issue>,
    ) => cmp('creatorId', '=', authData.sub);

    return {
      user: {
        row: {
          select: undefined,
          insert: [],
          update: {
            preMutation: [],
          },
          delete: [],
        },
      },
      issue: {
        row: {
          insert: [
            (
              authData: AuthData,
              eb: ExpressionBuilder<typeof schema.tables.issue>,
            ) =>
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
          },
          delete: [],
          select: [canSeeIssue],
        },
      },
      comment: {
        row: {
          insert: [
            (
              authData: AuthData,
              eb: ExpressionBuilder<typeof schema.tables.comment>,
            ) =>
              eb.and(
                isCommentCreator(authData, eb),
                canSeeComment(authData, eb),
              ),
          ],
          update: {
            preMutation: [isAdmin, isCommentCreator],
            // TODO (mlaw): ensure that the authorId is not changed
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
          update: {
            preMutation: [],
          },
          delete: [
            isAdmin,
            canWriteIssueLabelIfProjectMember,
            canWriteIssueLabelIfIssueCreator,
            canWriteIssueLabelIfIssueOwner,
          ],
        },
      },
      viewState: {
        row: {
          insert: [isViewStateOwner],
          update: {
            preMutation: [isViewStateOwner],
            postProposedMutation: [isViewStateOwner],
          },
          delete: [isViewStateOwner],
        },
      },
    };
  }),
);

let queryDelegate: QueryDelegate;
let replica: Database;
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
beforeEach(() => {
  replica = new Database(lc, ':memory:');
  const sources = new Map<string, Source>();
  queryDelegate = {
    getSource: (name: string) => {
      let source = sources.get(name);
      if (source) {
        return source;
      }
      const tableSchema = (
        schema.tables as unknown as Record<string, TableSchema>
      )[name];
      assert(tableSchema, `Table schema not found for ${name}`);

      // create the SQLite table
      replica.exec(`
      CREATE TABLE "${name}" (
        ${Object.entries(tableSchema.columns)
          .map(([name, c]) => `"${name}" ${toDbType(c.type)}`)
          .join(', ')},
        PRIMARY KEY (${tableSchema.primaryKey.map(k => `"${k}"`).join(', ')})
      )`);

      source = new TableSource(
        replica,
        name,
        tableSchema.columns,
        tableSchema.primaryKey,
      );

      sources.set(name, source);
      return source;
    },

    createStorage() {
      return new MemoryStorage();
    },
    addServerQuery() {
      return () => {};
    },
    onTransactionCommit() {
      return () => {};
    },
    batchViewUpdates<T>(applyViewUpdates: () => T): T {
      return applyViewUpdates();
    },
  };

  for (const table of Object.values(schema.tables)) {
    // force the sqlite tables to be created by getting all the sources
    must(queryDelegate.getSource(table.tableName));
  }

  writeAuthorizer = new WriteAuthorizerImpl(
    lc,
    {},
    schema,
    permissions,
    replica,
    'cg',
  );
});
const lc = createSilentLogContext();

test('cannot create an issue with the wrong creatorId, even if admin', () => {
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
    writeAuthorizer.canPreMutation(authData, ops) &&
      writeAuthorizer.canPostMutation(authData, ops),
  ).toBe(false);

  authData = {
    sub: '002',
    role: 'admin',
  };
  expect(
    writeAuthorizer.canPreMutation(authData, ops) &&
      writeAuthorizer.canPostMutation(authData, ops),
  ).toBe(true);
});

function addUser(user: TableSchemaToRow<typeof schema.tables.user>) {
  const userSource = must(queryDelegate.getSource('user'));
  userSource.push({
    type: 'add',
    row: user,
  });
}

function addProject(project: TableSchemaToRow<typeof schema.tables.project>) {
  const projectSource = must(queryDelegate.getSource('project'));
  projectSource.push({
    type: 'add',
    row: project,
  });
}

function addProjectMember(
  projectMember: TableSchemaToRow<typeof schema.tables.projectMember>,
) {
  const projectMemberSource = must(queryDelegate.getSource('projectMember'));
  projectMemberSource.push({
    type: 'add',
    row: projectMember,
  });
}

function addIssue(issue: TableSchemaToRow<typeof schema.tables.issue>) {
  const issueSource = must(queryDelegate.getSource('issue'));
  issueSource.push({
    type: 'add',
    row: issue,
  });
}

function addComment(comment: TableSchemaToRow<typeof schema.tables.comment>) {
  const commentSource = must(queryDelegate.getSource('comment'));
  commentSource.push({
    type: 'add',
    row: comment,
  });
}

function addLabel(label: TableSchemaToRow<typeof schema.tables.label>) {
  const labelSource = must(queryDelegate.getSource('label'));
  labelSource.push({
    type: 'add',
    row: label,
  });
}

function addIssueLabel(
  issueLabel: TableSchemaToRow<typeof schema.tables.issueLabel>,
) {
  const issueLabelSource = must(queryDelegate.getSource('issueLabel'));
  issueLabelSource.push({
    type: 'add',
    row: issueLabel,
  });
}

function addViewState(
  viewState: TableSchemaToRow<typeof schema.tables.viewState>,
) {
  const viewStateSource = must(queryDelegate.getSource('viewState'));
  viewStateSource.push({
    type: 'add',
    row: viewState,
  });
}

test('cannot create an issue unless you are a project member', () => {
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
    writeAuthorizer.canPreMutation(authData, [op]) &&
      writeAuthorizer.canPostMutation(authData, [op]),
  ).toBe(true);

  // user 2 is not a member of project 1
  op.value.creatorId = '002';
  authData = {sub: '002', role: 'user'};
  expect(
    writeAuthorizer.canPreMutation(authData, [op]) &&
      writeAuthorizer.canPostMutation(authData, [op]),
  ).toBe(false);

  // user 2 is a member of project 2
  op.value.projectId = '002';
  expect(
    writeAuthorizer.canPreMutation(authData, [op]) &&
      writeAuthorizer.canPostMutation(authData, [op]),
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

  test('update as project member', () => {
    const op: UpdateOp = {
      op: 'update',
      tableName: 'issue',
      primaryKey: ['id'],
      value: {id: '001', closed: true},
    };
    let authData = {sub: '001', role: 'user'};
    // user 1 is a member of project 1 so they can update the issue
    expect(
      writeAuthorizer.canPreMutation(authData, [op]) &&
        writeAuthorizer.canPostMutation(authData, [op]),
    ).toBe(true);

    // user 2 is not a project member (or owner or creator) of issue 1 so they cannot update the issue
    authData = {sub: '002', role: 'user'};
    expect(
      writeAuthorizer.canPreMutation(authData, [op]) &&
        writeAuthorizer.canPostMutation(authData, [op]),
    ).toBe(false);
  });

  test('update as creator', () => {
    const op: UpdateOp = {
      op: 'update',
      tableName: 'issue',
      primaryKey: ['id'],
      value: {id: '002', closed: true},
    };

    let authData = {sub: '001', role: 'user'};
    // user 1 is the creator of issue 2 so they can update the issue
    expect(
      writeAuthorizer.canPreMutation(authData, [op]) &&
        writeAuthorizer.canPostMutation(authData, [op]),
    ).toBe(true);

    // user 2 is not a creator (or owner or project member) of issue 2 so they cannot update the issue
    authData = {sub: '002', role: 'user'};
    expect(
      writeAuthorizer.canPreMutation(authData, [op]) &&
        writeAuthorizer.canPostMutation(authData, [op]),
    ).toBe(false);
  });

  test('update as owner', () => {
    const op: UpdateOp = {
      op: 'update',
      tableName: 'issue',
      primaryKey: ['id'],
      value: {id: '003', closed: true},
    };

    let authData = {sub: '001', role: 'user'};
    // user 1 is the owner of issue 3 so they can update the issue
    expect(
      writeAuthorizer.canPreMutation(authData, [op]) &&
        writeAuthorizer.canPostMutation(authData, [op]),
    ).toBe(true);

    // user 2 is not a owner (or creator or project member) of issue 3 so they cannot update the issue
    authData = {sub: '002', role: 'user'};
    expect(
      writeAuthorizer.canPreMutation(authData, [op]) &&
        writeAuthorizer.canPostMutation(authData, [op]),
    ).toBe(false);
  });

  test('update as admin', () => {
    const op: UpdateOp = {
      op: 'update',
      tableName: 'issue',
      primaryKey: ['id'],
      value: {id: '003', closed: true},
    };

    const authData = {sub: '005', role: 'admin'};
    expect(
      writeAuthorizer.canPreMutation(authData, [op]) &&
        writeAuthorizer.canPostMutation(authData, [op]),
    ).toBe(true);
  });

  test('view as admin', () => {
    // Admin can see all of the issues
    expect(
      runReadQueryWithPermissions(
        {sub: '005', role: 'admin'},
        newQuery(queryDelegate, schema.tables.issue),
      ).map(r => r.row.id),
    ).toEqual(['001', '002', '003']);
  });

  test('view as project member, creator or owner', () => {
    // user 1 is project member for issue 1, creator of issue 2 and owner of issue 3
    expect(
      runReadQueryWithPermissions(
        {sub: '001', role: 'user'},
        newQuery(queryDelegate, schema.tables.issue),
      ).map(r => r.row.id),
    ).toEqual(['001', '002', '003']);

    // user 2 is not a project member, creator or owner of any issues
    expect(
      runReadQueryWithPermissions(
        {sub: '002', role: 'user'},
        newQuery(queryDelegate, schema.tables.issue),
      ).map(r => r.row.id),
    ).toEqual([]);

    // user 3 is creator / owner of all issues
    expect(
      runReadQueryWithPermissions(
        {sub: '003', role: 'user'},
        newQuery(queryDelegate, schema.tables.issue),
      ).map(r => r.row.id),
    ).toEqual(['001', '002', '003']);

    // user 11 is only a member of project 1
    expect(
      runReadQueryWithPermissions(
        {sub: '011', role: 'user'},
        newQuery(queryDelegate, schema.tables.issue),
      ).map(r => r.row.id),
    ).toEqual(['001']);

    // user 12 is only a member of project 2
    expect(
      runReadQueryWithPermissions(
        {sub: '012', role: 'user'},
        newQuery(queryDelegate, schema.tables.issue),
      ).map(r => r.row.id),
    ).toEqual(['002', '003']);
  });

  test('cannot delete an issue', () => {
    const op: DeleteOp = {
      op: 'delete',
      tableName: 'issue',
      primaryKey: ['id'],
      value: {id: '003'},
    };

    for (const sub of ['001', '002', '003']) {
      const authData = {sub, role: 'user'};
      expect(
        writeAuthorizer.canPreMutation(authData, [op]) &&
          writeAuthorizer.canPostMutation(authData, [op]),
      ).toBe(false);
    }

    const authData = {sub: '005', role: 'admin'};
    expect(
      writeAuthorizer.canPreMutation(authData, [op]) &&
        writeAuthorizer.canPostMutation(authData, [op]),
    ).toBe(false);
  });
});

function ast(q: Query<TableSchema, QueryType>) {
  return (q as QueryImpl<TableSchema, QueryType>)[completedAstSymbol];
}

function runReadQueryWithPermissions(
  authData: AuthData,
  query: Query<TableSchema, QueryType>,
) {
  const updatedAst = bindStaticParameters(
    must(transformQuery(ast(query), permissions)),
    {
      authData,
      preMutationRow: undefined,
    },
  );
  const pipeline = buildPipeline(updatedAst, queryDelegate);
  const out = new Catch(pipeline);
  return out.fetch({});
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

  test('cannot set authorId to another user for a comment on insert', () => {
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
      writeAuthorizer.canPreMutation(authData, [op]) &&
        writeAuthorizer.canPostMutation(authData, [op]),
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
      writeAuthorizer.canPreMutation(authData, [op]) &&
        writeAuthorizer.canPostMutation(authData, [op]),
    ).toBe(true);
  });

  test('cannot create a comment for an issue you cannot see', () => {
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
      writeAuthorizer.canPreMutation(authData, [op]) &&
        writeAuthorizer.canPostMutation(authData, [op]),
    ).toBe(false);

    // upgrading user 4 to admin should allow them to see the issue and write the comment
    authData = {sub: '004', role: 'admin'};
    expect(
      writeAuthorizer.canPreMutation(authData, [op]) &&
        writeAuthorizer.canPostMutation(authData, [op]),
    ).toBe(true);
  });

  test('cannot update a comment unless you created the comment or are the admin', () => {
    let op: UpdateOp = {
      op: 'update',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: '001', text: 'updated comment'},
    };
    // user 2 did not create comment 1
    const authData = {sub: '002', role: 'user'};
    expect(
      writeAuthorizer.canPreMutation(authData, [op]) &&
        writeAuthorizer.canPostMutation(authData, [op]),
    ).toBe(false);

    // user 2 did create comment 2
    op = {
      op: 'update',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: '002', text: 'updated comment'},
    };
    expect(
      writeAuthorizer.canPreMutation(authData, [op]) &&
        writeAuthorizer.canPostMutation(authData, [op]),
    ).toBe(true);
  });

  test('cannot delete a comment unless you are the admin or the author of the comment', () => {
    let op: DeleteOp = {
      op: 'delete',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: '001'},
    };
    let authData = {sub: '002', role: 'user'};
    // user 2 did not create comment 1
    expect(
      writeAuthorizer.canPreMutation(authData, [op]) &&
        writeAuthorizer.canPostMutation(authData, [op]),
    ).toBe(false);

    // user 2 did create comment 2
    op = {
      op: 'delete',
      tableName: 'comment',
      primaryKey: ['id'],
      value: {id: '002'},
    };
    expect(
      writeAuthorizer.canPreMutation(authData, [op]) &&
        writeAuthorizer.canPostMutation(authData, [op]),
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
      writeAuthorizer.canPreMutation(authData, [op]) &&
        writeAuthorizer.canPostMutation(authData, [op]),
    ).toBe(true);
  });

  test('cannot see a comment unless you can see the issue', () => {
    // users 1, 2 and 3 and 5 can see all comments because they can see the issue
    // user 4 cannot see any comments because they cannot see any issues
    for (const sub of ['001', '002', '003', '005']) {
      expect(
        runReadQueryWithPermissions(
          {sub, role: sub === '005' ? 'admin' : 'user'},
          newQuery(queryDelegate, schema.tables.comment),
        ).map(r => r.row.id),
      ).toEqual(['001', '002']);
    }

    expect(
      runReadQueryWithPermissions(
        {sub: '004', role: 'user'},
        newQuery(queryDelegate, schema.tables.comment),
      ).map(r => r.row.id),
    ).toEqual([]);
  });

  test('cannot insert an issueLabel if not admin/project-member/issue-creator/issue-owner', () => {
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
        writeAuthorizer.canPreMutation(authData, [op]) &&
          writeAuthorizer.canPostMutation(authData, [op]),
      ).toBe(false);

      // upgrading user 4 to admin should allow them to see the issue and write the issueLabel
      authData = {sub: '004', role: 'admin'};
      expect(
        writeAuthorizer.canPreMutation(authData, [op]) &&
          writeAuthorizer.canPostMutation(authData, [op]),
      ).toBe(true);

      for (const sub of ['001', '002', '003']) {
        authData = {sub, role: 'user'};
        expect(
          writeAuthorizer.canPreMutation(authData, [op]) &&
            writeAuthorizer.canPostMutation(authData, [op]),
        ).toBe(true);
      }
    }
  });
});

test('can only insert a viewState if you are the owner', () => {
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
      writeAuthorizer.canPreMutation(authData, [op]) &&
        writeAuthorizer.canPostMutation(authData, [op]),
    ).toBe(true);

    // user 2 cannot insert/update/delete a viewState for user 1
    authData = {sub: '002', role: 'user'};
    expect(
      writeAuthorizer.canPreMutation(authData, [op]) &&
        writeAuthorizer.canPostMutation(authData, [op]),
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
      query: newQuery(queryDelegate, schema.tables.user)
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
      query: newQuery(queryDelegate, schema.tables.user)
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
      query: newQuery(queryDelegate, schema.tables.user)
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
      query: newQuery(queryDelegate, schema.tables.user).related(
        'projects',
        q => q.related('issues', q => q.related('comments')),
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
    );
    expect(toIdsOnly(actual)).toEqual(expected);
  });
});

// maps over nodes, drops all information from `row` except the id
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function toIdsOnly(nodes: Node[]): any[] {
  return nodes.map(node => {
    return {
      id: node.row.id,
      ...Object.fromEntries(
        Object.entries(node.relationships)
          .filter(([k]) => !k.startsWith('zsubq_'))
          .map(([k, v]) => [k, toIdsOnly(Array.isArray(v) ? v : [...v])]),
      ),
    };
  });
}

// TODO (mlaw): test that `exists` does not provide an oracle
