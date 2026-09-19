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
});
