import {describe, expect, test} from 'vitest';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {makeSourceChangeRemove} from './source.ts';
import {
  runPushTest,
  type SourceContents,
  type Sources,
} from './test/fetch-and-push-tests.ts';
import type {Format} from './view.ts';

describe('runaway push reproduction', () => {
  const sources: Sources = {
    issue: {
      columns: {
        id: {type: 'string'},
        projectID: {type: 'string'},
      },
      primaryKeys: ['id'],
    },
    project: {
      columns: {
        id: {type: 'string'},
        name: {type: 'string'},
      },
      primaryKeys: ['id'],
    },
  };

  const issues = Array.from({length: 100}, (_, i) => ({
    id: `i${String(i).padStart(3, '0')}`,
    projectID: 'p1',
  }));

  const sourceContents: SourceContents = {
    issue: issues,
    project: [{id: 'p1', name: 'Alpha'}],
  };

  const ast: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      flip: true,
      related: {
        system: 'client',
        correlation: {parentField: ['projectID'], childField: ['id']},
        subquery: {
          table: 'project',
          alias: 'project',
          orderBy: [['id', 'asc']],
        },
      },
    },
    limit: 5,
  };

  const format: Format = {
    singular: false,
    relationships: {},
  };

  test('child change in FlippedJoin causes unbounded push to downstream Take', () => {
    const {log} = runPushTest({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [['project', makeSourceChangeRemove({id: 'p1', name: 'Alpha'})]],
    });

    const flippedJoinPushes = log.filter(
      entry => entry[0] === ':flipped-join(project)' && entry[1] === 'push',
    );
    const takePushes = log.filter(
      entry => entry[0] === ':take' && entry[1] === 'push',
    );

    // With TakeGate, only 5 matching parent rows (bounded by Take's limit)
    // are fetched by FlippedJoin and pushed downstream into Take!
    expect(flippedJoinPushes.length).toBe(5);
    expect(takePushes.length).toBeLessThanOrEqual(10);
  });

  test('multiple removals are batched in Phase 1 before deficit refill in Phase 2', () => {
    const customSourceContents: SourceContents = {
      issue: [
        {id: 'i000', projectID: 'p1'},
        {id: 'i001', projectID: 'p1'},
        {id: 'i002', projectID: 'p1'},
        {id: 'i003', projectID: 'p2'},
        {id: 'i004', projectID: 'p2'},
        {id: 'i005', projectID: 'p2'},
        {id: 'i006', projectID: 'p2'},
      ],
      project: [
        {id: 'p1', name: 'Alpha'},
        {id: 'p2', name: 'Beta'},
      ],
    };

    const {pushes, data} = runPushTest({
      sources,
      sourceContents: customSourceContents,
      ast,
      format,
      pushes: [['project', makeSourceChangeRemove({id: 'p1', name: 'Alpha'})]],
    });

    // In Phase 1, all 3 p1 issues are removed without interleaving refills.
    // In Phase 2 (reconcile), Take refills the 3-row deficit by fetching i005 and i006 in a single batch.
    expect(
      pushes.map(p =>
        p.type === 'add' || p.type === 'remove'
          ? [p.type, p.node !== 'yield' ? (p.node.row.id as string) : 'yield']
          : [p.type],
      ),
    ).toEqual([
      ['remove', 'i000'],
      ['remove', 'i001'],
      ['remove', 'i002'],
      ['add', 'i005'],
      ['add', 'i006'],
    ]);

    expect(data).toMatchObject([
      {id: 'i003', projectID: 'p2'},
      {id: 'i004', projectID: 'p2'},
      {id: 'i005', projectID: 'p2'},
      {id: 'i006', projectID: 'p2'},
    ]);
  });

  test('nested exists with CapGate bounds intermediate parent fetches', () => {
    const nestedSources: Sources = {
      project: {
        columns: {
          id: {type: 'string'},
          name: {type: 'string'},
        },
        primaryKeys: ['id'],
      },
      issue: {
        columns: {
          id: {type: 'string'},
          projectID: {type: 'string'},
        },
        primaryKeys: ['id'],
      },
      comment: {
        columns: {
          id: {type: 'string'},
          issueID: {type: 'string'},
        },
        primaryKeys: ['id'],
      },
    };

    const nestedIssues = Array.from({length: 100}, (_, i) => ({
      id: `i${String(i).padStart(3, '0')}`,
      projectID: 'p1',
    }));

    const nestedSourceContents: SourceContents = {
      project: [{id: 'p1', name: 'Alpha'}],
      issue: nestedIssues,
      comment: [{id: 'c1', issueID: 'i000'}],
    };

    // Query: project WHERE EXISTS (issue WHERE EXISTS (comment))
    const nestedAst: AST = {
      table: 'project',
      orderBy: [['id', 'asc']],
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        flip: false,
        related: {
          system: 'client',
          correlation: {parentField: ['id'], childField: ['projectID']},
          subquery: {
            table: 'issue',
            alias: 'issues',
            where: {
              type: 'correlatedSubquery',
              op: 'EXISTS',
              flip: false,
              related: {
                system: 'client',
                correlation: {parentField: ['id'], childField: ['issueID']},
                subquery: {
                  table: 'comment',
                  alias: 'comments',
                },
              },
            },
          },
        },
      },
    };

    const {log, data} = runPushTest({
      sources: nestedSources,
      sourceContents: nestedSourceContents,
      ast: nestedAst,
      format,
      pushes: [
        ['comment', makeSourceChangeRemove({id: 'c1', issueID: 'i000'})],
      ],
    });

    const joinPushes = log.filter(
      entry => entry[0]?.includes('join') && entry[1] === 'push',
    );
    expect(data).toEqual([]);
    expect(joinPushes.length).toBeLessThanOrEqual(2);
  });
});
