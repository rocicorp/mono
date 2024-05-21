import {beforeEach, describe, expect, test} from 'vitest';
import {
  makeInfiniteSourceContext,
  makeTestContext,
  TestContext,
} from '../context/test-context.js';
import type {Source} from '../ivm/source/source.js';
import {EntityQuery, exp, or} from './entity-query.js';
import * as agg from './agg.js';

describe('a limited window is correctly maintained over differences', () => {
  type E = {
    id: string;
  };
  const letters = 'abcdefghijklmnopqrstuvwxyz'.split('');

  let context: TestContext;
  let source: Source<E>;
  let q: EntityQuery<{e: E}>;
  beforeEach(() => {
    context = makeTestContext();
    source = context.getSource<E>('e');
    q = new EntityQuery<{e: E}>(context, 'e');
    Array.from({length: 10}, (_, i) => source.add({id: letters[i * 2 + 3]}));
  });

  test('adding values below the established window (asc)', async () => {
    const stmt = q.select('id').limit(5).prepare();
    const data = await stmt.exec();

    expect(data.map(x => x.id)).toEqual(['d', 'f', 'h', 'j', 'l']);

    source.add({id: 'c'});
    const newData = await stmt.exec();

    // if we are limited and in ASC order, things below MIN are added to the window
    expect(newData.map(x => x.id)).toEqual(['c', 'd', 'f', 'h', 'j']);
  });

  test('adding values above the established window (asc)', async () => {
    const stmt = q.select('id').limit(5).prepare();
    const data = await stmt.exec();

    expect(data.map(x => x.id)).toEqual(['d', 'f', 'h', 'j', 'l']);

    source.add({id: 'p'});
    const newData = await stmt.exec();

    // if we are limited and in ASC order, things above MAX are not added to the window
    expect(newData.map(x => x.id)).toEqual(['d', 'f', 'h', 'j', 'l']);
  });

  test('adding values above the established window (desc)', async () => {
    const stmt = q.select('id').desc('id').limit(5).prepare();
    const data = await stmt.exec();

    expect(data.map(x => x.id)).toEqual(['v', 't', 'r', 'p', 'n']);

    source.add({id: 'z'});
    const newData = await stmt.exec();

    // if we are limited and in DESC order, things above MAX are added to the window
    expect(newData.map(x => x.id)).toEqual(['z', 'v', 't', 'r', 'p']);

    stmt.destroy();
  });

  test('adding values below the established window (desc)', async () => {
    const stmt = q.select('id').desc('id').limit(5).prepare();
    const data = await stmt.exec();

    expect(data.map(x => x.id)).toEqual(['v', 't', 'r', 'p', 'n']);

    source.add({id: 'a'});
    const newData = await stmt.exec();

    // if we are limited and in DESC order, things below MIN are not added to the window
    expect(newData.map(x => x.id)).toEqual(['v', 't', 'r', 'p', 'n']);

    stmt.destroy();
  });

  test('adding values inside the established window (asc)', async () => {
    const stmt = q.select('id').limit(5).prepare();
    const data = await stmt.exec();

    expect(data.map(x => x.id)).toEqual(['d', 'f', 'h', 'j', 'l']);

    source.add({id: 'i'});
    const newData = await stmt.exec();

    // if we are limited and in ASC order, things inside the window are added
    expect(newData.map(x => x.id)).toEqual(['d', 'f', 'h', 'i', 'j']);

    stmt.destroy();
  });

  test('adding values inside the established window (desc)', async () => {
    const stmt = q.select('id').desc('id').limit(5).prepare();
    const data = await stmt.exec();

    expect(data.map(x => x.id)).toEqual(['v', 't', 'r', 'p', 'n']);

    source.add({id: 'q'});
    const newData = await stmt.exec();

    // if we are limited and in DESC order, things inside the window are added
    expect(newData.map(x => x.id)).toEqual(['v', 't', 'r', 'q', 'p']);

    stmt.destroy();
  });
});

function numToPaddedString(i: number) {
  const str = String(i);
  return '0'.repeat(10 - str.length) + str;
}

/**
 * To make sure `limit` is actually `limiting` the amount of data we're processing
 * from a source, we need to test it with an infinite source.
 */
describe('pulling from an infinite source is possible if we set a limit', () => {
  type E = {
    id: string;
  };
  const infiniteGenerator = {
    *[Symbol.iterator]() {
      let i = 0;
      while (true) {
        yield [{id: numToPaddedString(++i)}, 1] as const;
      }
    },
  };
  const numUsers = 20;
  const numLabels = 10;
  const infiniteIssueGenerator = {
    *[Symbol.iterator]() {
      let i = 0;
      while (true) {
        yield [
          {
            id: numToPaddedString(++i),
            title: `Issue ${numToPaddedString(i)}`,
            ownerId: numToPaddedString(((i - 1) % numUsers) + 1),
          } satisfies Issue,
          1,
        ] as const;
      }
    },
  };

  const generators = new Map([
    ['e', infiniteGenerator],
    ['issue', infiniteIssueGenerator],
  ]);

  type Issue = {
    id: string;
    title: string;
    ownerId: string;
  };
  type IssueLabel = {
    id: string;
    issueId: string;
    labelId: string;
  };
  type Label = {
    id: string;
    name: string;
  };
  type User = Label;

  const context = makeInfiniteSourceContext(generators);
  const issueLabelSource = context.getSource<IssueLabel>('issueLabel');
  const labelSource = context.getSource<Label>('label');
  // We can't do inifnite users or comments yet
  // or anything that appears on the right side of a join (yet).
  // We'd need to register a foreign key index for those sources so
  // we don't have to scan inifinitely.
  const userSource = context.getSource<User>('user');

  context.materialite.tx(() => {
    // we obviously can't cover the infinite set of issues
    // till we have `infiniteIndex` support
    let labelId = 0;
    for (let i = 0; i < 100; i++) {
      if (i % 2 === 0) {
        continue;
      }
      const label1 = {
        id: numToPaddedString(labelId + 1),
        issueId: numToPaddedString(i + 1),
        labelId: numToPaddedString((labelId % numLabels) + 1),
      };
      ++labelId;
      const label2 = {
        id: numToPaddedString(labelId + 1),
        issueId: numToPaddedString(i + 1),
        labelId: numToPaddedString((labelId % numLabels) + 1),
      };
      ++labelId;
      const label3 = {
        id: numToPaddedString(labelId + 1),
        issueId: numToPaddedString(i + 1),
        labelId: numToPaddedString((labelId % numLabels) + 1),
      };
      ++labelId;
      issueLabelSource.add(label1);
      issueLabelSource.add(label2);
      issueLabelSource.add(label3);
    }

    for (let i = 0; i < numLabels; i++) {
      labelSource.add({
        id: numToPaddedString(i + 1),
        name: `Label ${numToPaddedString(i + 1)}`,
      });
    }

    for (let i = 0; i < numUsers; i++) {
      userSource.add({
        id: numToPaddedString(i + 1),
        name: `User ${numToPaddedString(i + 1)}`,
      });
    }
  });

  test('bare select', async () => {
    const q = new EntityQuery<{e: E}>(context, 'e');
    const stmt = q.select('id').limit(2).prepare();
    const data = await stmt.exec();

    expect(data).toEqual([
      {id: numToPaddedString(1)},
      {id: numToPaddedString(2)},
    ]);

    stmt.destroy();
  });

  test('select and where', async () => {
    const q = new EntityQuery<{e: E}>(context, 'e');
    const stmt = q
      .select('id')
      .where('e.id', '>', numToPaddedString(9))
      .limit(2)
      .prepare();
    const data = await stmt.exec();

    expect(data).toEqual([
      {id: numToPaddedString(10)},
      {id: numToPaddedString(11)},
    ]);

    stmt.destroy();
  });

  test('select and where with or', async () => {
    const q = new EntityQuery<{e: E}>(context, 'e');
    const stmt = q
      .select('id')
      .where(
        or(
          exp('e.id', '>', numToPaddedString(9)),
          exp('e.id', '>', numToPaddedString(8)),
        ),
      )
      .limit(15)
      .prepare();
    const data = await stmt.exec();

    expect(data).toEqual(
      Array.from({length: 15}, (_, i) => ({id: numToPaddedString(9 + i)})),
    );

    stmt.destroy();
  });

  const issueQuery = new EntityQuery<{issue: Issue}>(context, 'issue');
  const userQuery = new EntityQuery<{user: User}>(context, 'user');
  const issueLabelQuery = new EntityQuery<{issueLabel: IssueLabel}>(
    context,
    'issueLabel',
  );
  const labelQuery = new EntityQuery<{label: Label}>(context, 'label');
  test('bare select with a join', async () => {
    const limit = 50;
    const stmt = issueQuery
      .join(userQuery, 'user', 'ownerId', 'id')
      .select('*')
      .limit(limit)
      .prepare();

    const data = await stmt.exec();
    checkIssueUsers(data, limit);
    stmt.destroy();
  });

  // this should be the same as join, no?
  test('bare select with a left join', async () => {
    const limit = 50;
    const stmt = issueQuery
      .leftJoin(userQuery, 'user', 'ownerId', 'id')
      .select('*')
      .limit(limit)
      .prepare();

    const data = await stmt.exec();
    checkIssueUsers(data, limit);
    stmt.destroy();
  });

  // Odd issues have no labels, even issues have 3 labels.
  // We should get back LIMIT / 3 unique issues with 3 labels each.
  // Each issue ID being even.
  test('1:many join with missing rows', async () => {
    const limit = 10;
    const stmt = issueQuery
      .join(issueLabelQuery, 'issueLabel', 'id', 'issueId')
      .select('*')
      .limit(limit)
      .prepare();

    const data = await stmt.exec();
    const expected = Array.from({length: limit}, (_, i) => ({
      id: numToPaddedString((Math.floor(i / 3) + 1) * 2),
      labels: numToPaddedString((i % numLabels) + 1),
    }));
    const result = data.map(x => ({
      id: x.issue.id,
      labels: x.issueLabel ? x.issueLabel.labelId : 'none',
    }));
    expect(result).toEqual(expected);
    stmt.destroy();
  });

  // Odd issues have no labels, even issues have 3 labels.
  // We should get back LIMIT - (LIMIT / 6) unique issues.
  // Odd issues with no labels
  // Even ones with 3 labels
  // The issue source is infinite so this checks that we don't scan the entire source
  test('1:many left join with missing rows', async () => {
    const limit = 10;
    const stmt = issueQuery
      .leftJoin(issueLabelQuery, 'issueLabel', 'id', 'issueId')
      .select('*')
      .limit(limit)
      .prepare();

    const data = await stmt.exec();
    let expectedLabelId = 0;
    const expected = Array.from({length: limit})
      .flatMap((_, i) => {
        if (i % 2 === 0) {
          return [
            {
              id: numToPaddedString(i + 1),
              labels: 'none',
            },
          ];
        }
        return Array.from({length: 3}, () => ({
          id: numToPaddedString(i + 1),
          labels: numToPaddedString((expectedLabelId++ % numLabels) + 1),
        }));
      })
      .slice(0, limit);
    const result = data.map(x => ({
      id: x.issue.id,
      labels: x.issueLabel ? x.issueLabel.labelId : 'none',
    }));
    expect(result).toEqual(expected);
    stmt.destroy();
  });

  test('agg_array with a join', async () => {
    const limit = 10;
    const stmt = issueQuery
      .leftJoin(issueLabelQuery, 'issueLabel', 'id', 'issueId')
      .leftJoin(labelQuery, 'label', 'issueLabel.labelId', 'label.id')
      .groupBy('issue.id')
      .limit(limit)
      .select('issue.id', agg.array('label.name', 'labels'))
      .prepare();

    const data = await stmt.exec();

    let j = 0;
    const expected = Array.from({length: limit}, (_, i) => ({
      id: numToPaddedString(i + 1),
      labels:
        i % 2 === 0
          ? []
          : [
              'Label ' + numToPaddedString((j++ % numLabels) + 1),
              'Label ' + numToPaddedString((j++ % numLabels) + 1),
              'Label ' + numToPaddedString((j++ % numLabels) + 1),
            ],
    }));
    const result = data.map(x => ({
      id: x.issue.id,
      labels: x.labels,
    }));

    expect(result).toEqual(expected);
    stmt.destroy();
  });

  function checkIssueUsers(
    data: readonly {
      readonly issue: Issue;
      readonly user?: User | undefined;
    }[],
    limit: number,
  ) {
    const expected = Array.from({length: limit}, (_, i) => ({
      id: numToPaddedString(i + 1),
      owner: `User ${numToPaddedString((i % numUsers) + 1)}`,
    }));
    expect(data.map(x => ({id: x.issue.id, owner: x.user?.name}))).toEqual(
      expected,
    );
  }
});
