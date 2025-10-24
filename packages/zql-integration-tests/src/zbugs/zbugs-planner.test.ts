// oxlint-disable expect-expect
// oxlint-disable no-console
// @ts-nocheck - This file is for debugging query plans only
import {beforeAll, describe, test} from 'vitest';
import {createSQLiteCostModel} from '../../../zqlite/src/sqlite-cost-model.ts';
import {
  clientToServer,
  type NameMapper,
} from '../../../zero-schema/src/name-mapper.ts';
import {makeGetPlanAST} from '../helpers/planner.ts';
import {computeZqlSpecs} from '../../../zero-cache/src/db/lite-tables.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {LiteAndZqlSpec} from '../../../zero-cache/src/db/specs.ts';
import {schema} from '../../../../apps/zbugs/shared/schema.ts';
import {AccumulatorDebugger} from '../../../zql/src/planner/planner-debug.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {newQueryDelegate} from '../../../zqlite/src/test/source-factory.ts';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {QueryImpl} from '../../../zql/src/query/query-impl.ts';
import {defaultFormat} from '../../../zql/src/ivm/default-format.ts';

// Path to the zbugs replica database
const ZBUGS_REPLICA_PATH =
  '/Users/mlaw/workspace/mono/apps/zbugs/zbugs-replica.db';

let costModel: ReturnType<typeof createSQLiteCostModel>;
let mapper: NameMapper;
let getPlanAST: ReturnType<typeof makeGetPlanAST>;
let db: Database;
// oxlint-disable-next-line @typescript-eslint/no-explicit-any
let q: any;

describe('ZBugs Query Planner Analysis', () => {
  beforeAll(() => {
    const lc = createSilentLogContext();
    // Open the zbugs replica database
    db = new Database(lc, ZBUGS_REPLICA_PATH);

    mapper = clientToServer(schema.tables);
    db.exec('ANALYZE;');

    // Get table specs using computeZqlSpecs
    const tableSpecs = new Map<string, LiteAndZqlSpec>();
    computeZqlSpecs(lc, db, tableSpecs);

    costModel = createSQLiteCostModel(db, tableSpecs);

    getPlanAST = makeGetPlanAST(mapper, costModel);

    // Create query instances with delegates
    const delegate = newQueryDelegate(lc, testLogConfig, db, schema);
    q = {} as typeof q;
    for (const table of Object.keys(schema.tables) as Array<
      keyof typeof schema.tables
    >) {
      q[table] = new QueryImpl(
        delegate,
        schema,
        table,
        {table},
        defaultFormat,
        'test',
      );
    }
  });

  test('allLabels', () => {
    const planDebugger = new AccumulatorDebugger();
    getPlanAST(q.label, planDebugger);
    console.log('\n=== allLabels Query Plan ===');
    console.log(planDebugger.format());
  });

  test('allUsers', () => {
    const planDebugger = new AccumulatorDebugger();
    getPlanAST(q.user, planDebugger);
    console.log('\n=== allUsers Query Plan ===');
    console.log(planDebugger.format());
  });

  test('allProjects', () => {
    const planDebugger = new AccumulatorDebugger();
    getPlanAST(q.project, planDebugger);
    console.log('\n=== allProjects Query Plan ===');
    console.log(planDebugger.format());
  });

  test('user by id', () => {
    const planDebugger = new AccumulatorDebugger();
    getPlanAST(q.user.where('id', 'test-user-id').one(), planDebugger);
    console.log('\n=== user by id Query Plan ===');
    console.log(planDebugger.format());
  });

  test('labelsForProject', () => {
    const planDebugger = new AccumulatorDebugger();
    const projectName = 'Zero';
    getPlanAST(
      q.label.whereExists('project', q =>
        q.where('lowerCaseName', projectName.toLowerCase()),
      ),
      planDebugger,
    );
    console.log('\n=== labelsForProject Query Plan ===');
    console.log(planDebugger.format());
  });

  test('issuePreloadV2', () => {
    const planDebugger = new AccumulatorDebugger();
    const userID = 'test-user-id';
    const projectName = 'Zero';

    getPlanAST(
      q.issue
        .whereExists('project', p =>
          p.where('lowerCaseName', projectName.toLowerCase()),
        )
        .related('labels')
        .related('viewState', q => q.where('userID', userID))
        .related('creator')
        .related('assignee')
        .related('emoji', emoji => emoji.related('creator'))
        .related('comments', comments =>
          comments
            .related('creator')
            .related('emoji', emoji => emoji.related('creator'))
            .limit(10)
            .orderBy('created', 'desc'),
        )
        .orderBy('modified', 'desc')
        .orderBy('id', 'desc')
        .limit(1000),
      planDebugger,
    );
    console.log('\n=== issuePreloadV2 Query Plan ===');
    console.log(planDebugger.format());
  });

  test('userPref', () => {
    const planDebugger = new AccumulatorDebugger();
    const key = 'test-key';
    const userID = 'test-user-id';

    getPlanAST(
      q.userPref.where('key', key).where('userID', userID).one(),
      planDebugger,
    );
    console.log('\n=== userPref Query Plan ===');
    console.log(planDebugger.format());
  });

  test('userPickerV2 - crew filter', () => {
    const planDebugger = new AccumulatorDebugger();

    getPlanAST(
      q.user.where(({cmp, not, and}) =>
        and(cmp('role', 'crew'), not(cmp('login', 'LIKE', 'rocibot%'))),
      ),
      planDebugger,
    );
    console.log('\n=== userPickerV2 (crew filter) Query Plan ===');
    console.log(planDebugger.format());
  });

  test('userPickerV2 - creators filter', () => {
    const planDebugger = new AccumulatorDebugger();
    const projectName = 'Zero';

    getPlanAST(
      q.user.whereExists('createdIssues', i =>
        i.whereExists('project', p =>
          p.where('lowerCaseName', projectName.toLowerCase()),
        ),
      ),
      planDebugger,
    );
    console.log('\n=== userPickerV2 (creators filter) Query Plan ===');
    console.log(planDebugger.format());
  });

  test.only('userPickerV2 - assignees filter', () => {
    const planDebugger = new AccumulatorDebugger();
    const projectName = 'Zero';

    const ast = getPlanAST(
      q.user.whereExists('assignedIssues', i =>
        i.whereExists('project', p =>
          p.where('lowerCaseName', projectName.toLowerCase()),
        ),
      ),
      planDebugger,
    );
    console.log(JSON.stringify(ast, null, 2));
    console.log('\n=== userPickerV2 (assignees filter) Query Plan ===');
    // Note to self:
    // the greedy algorithm fails to find the best path
    // I think we should likely do all 2^n combinations of joins.
    // if n > 12 ....
    // ask the user to manually plan?
    // cache the plan?
    // heuristics to prune the search space???

    console.log(planDebugger.format());
  });

  test('issueDetail', () => {
    const planDebugger = new AccumulatorDebugger();
    const userID = 'test-user-id';

    getPlanAST(
      q.issue
        .where('id', 'test-issue-id')
        .related('project')
        .related('emoji', emoji => emoji.related('creator'))
        .related('creator')
        .related('assignee')
        .related('labels')
        .related('notificationState', q => q.where('userID', userID))
        .related('viewState', viewState =>
          viewState.where('userID', userID).one(),
        )
        .related('comments', comments =>
          comments
            .related('creator')
            .related('emoji', emoji => emoji.related('creator'))
            .limit(11) // INITIAL_COMMENT_LIMIT + 1
            .orderBy('created', 'desc')
            .orderBy('id', 'desc'),
        )
        .one(),
      planDebugger,
    );
    console.log('\n=== issueDetail Query Plan ===');
    console.log(planDebugger.format());
  });

  test('issueListV2 - open issues', () => {
    const planDebugger = new AccumulatorDebugger();
    const userID = 'test-user-id';
    const projectName = 'Zero';
    const limit = 50;

    getPlanAST(
      q.issue
        .whereExists('project', q =>
          q.where('lowerCaseName', projectName.toLowerCase()),
        )
        .related('viewState', q => q.where('userID', userID).one())
        .related('labels')
        .where('open', true)
        .orderBy('modified', 'desc')
        .orderBy('id', 'desc')
        .limit(limit),
      planDebugger,
    );
    console.log('\n=== issueListV2 (open issues) Query Plan ===');
    console.log(planDebugger.format());
  });

  test('issueListV2 - with text filter', () => {
    const planDebugger = new AccumulatorDebugger();
    const userID = 'test-user-id';
    const projectName = 'Zero';
    const textFilter = 'bug';
    const limit = 50;

    getPlanAST(
      q.issue
        .whereExists('project', q =>
          q.where('lowerCaseName', projectName.toLowerCase()),
        )
        .related('viewState', q => q.where('userID', userID).one())
        .related('labels')
        .where(({or, cmp, exists}) =>
          or(
            cmp('title', 'ILIKE', `%${textFilter}%`),
            cmp('description', 'ILIKE', `%${textFilter}%`),
            exists('comments', q =>
              q.where('body', 'ILIKE', `%${textFilter}%`),
            ),
          ),
        )
        .orderBy('modified', 'desc')
        .orderBy('id', 'desc')
        .limit(limit),
      planDebugger,
    );
    console.log('\n=== issueListV2 (with text filter) Query Plan ===');
    console.log(planDebugger.format());
  });

  test('issueListV2 - with creator filter', () => {
    const planDebugger = new AccumulatorDebugger();
    const userID = 'test-user-id';
    const projectName = 'Zero';
    const creatorLogin = 'testuser';
    const limit = 50;

    getPlanAST(
      q.issue
        .whereExists('project', q =>
          q.where('lowerCaseName', projectName.toLowerCase()),
        )
        .related('viewState', q => q.where('userID', userID).one())
        .related('labels')
        .whereExists('creator', q => q.where('login', creatorLogin))
        .orderBy('modified', 'desc')
        .orderBy('id', 'desc')
        .limit(limit),
      planDebugger,
    );
    console.log('\n=== issueListV2 (with creator filter) Query Plan ===');
    console.log(planDebugger.format());
  });

  test('issueListV2 - with assignee filter', () => {
    const planDebugger = new AccumulatorDebugger();
    const userID = 'test-user-id';
    const projectName = 'Zero';
    const assigneeLogin = 'testuser';
    const limit = 50;

    getPlanAST(
      q.issue
        .whereExists('project', q =>
          q.where('lowerCaseName', projectName.toLowerCase()),
        )
        .related('viewState', q => q.where('userID', userID).one())
        .related('labels')
        .whereExists('assignee', q => q.where('login', assigneeLogin))
        .orderBy('modified', 'desc')
        .orderBy('id', 'desc')
        .limit(limit),
      planDebugger,
    );
    console.log('\n=== issueListV2 (with assignee filter) Query Plan ===');
    console.log(planDebugger.format());
  });

  test('issueListV2 - with label filter', () => {
    const planDebugger = new AccumulatorDebugger();
    const userID = 'test-user-id';
    const projectName = 'Zero';
    const labelName = 'bug';
    const limit = 50;

    getPlanAST(
      q.issue
        .whereExists('project', q =>
          q.where('lowerCaseName', projectName.toLowerCase()),
        )
        .related('viewState', q => q.where('userID', userID).one())
        .related('labels')
        .whereExists('labels', q => q.where('name', labelName))
        .orderBy('modified', 'desc')
        .orderBy('id', 'desc')
        .limit(limit),
      planDebugger,
    );
    console.log('\n=== issueListV2 (with label filter) Query Plan ===');
    console.log(planDebugger.format());
  });

  test('emojiChange', () => {
    const planDebugger = new AccumulatorDebugger();
    const subjectID = 'test-subject-id';

    getPlanAST(
      q.emoji
        .where('subjectID', subjectID)
        .related('creator', creator => creator.one()),
      planDebugger,
    );
    console.log('\n=== emojiChange Query Plan ===');
    console.log(planDebugger.format());
  });

  test('playlist with track (many-to-many)', () => {
    const planDebugger = new AccumulatorDebugger();

    // This simulates a many-to-many relationship like issue -> labels
    getPlanAST(q.issue.whereExists('labels').limit(100), planDebugger);
    console.log('\n=== playlist with track (many-to-many) Query Plan ===');
    console.log(planDebugger.format());
  });
});
