import {describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {must} from '../../../shared/src/must.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {Catch} from '../ivm/catch.ts';
import type {Source} from '../ivm/source.ts';
import {makeSourceChangeAdd, makeSourceChangeEdit} from '../ivm/source.ts';
import {consume} from '../ivm/stream.ts';
import {createSource} from '../ivm/test/source-factory.ts';
import {newQuery} from './query-impl.ts';
import type {Query} from './query.ts';
import {QueryDelegateImpl} from './test/query-delegate.ts';

const lc = createSilentLogContext();

const issue = table('issue')
  .columns({id: number(), title: string().optional()})
  .primaryKey('id');
const issueLabel = table('issueLabel')
  .columns({id: number(), issueID: number(), labelID: number()})
  .primaryKey('id');
const label = table('label')
  .columns({id: number(), ownerID: number()})
  .primaryKey('id');
const owner = table('owner')
  .columns({id: number(), name: string()})
  .primaryKey('id');
const comment = table('comment')
  .columns({id: number(), issueID: number(), authorID: number()})
  .primaryKey('id');

const schema = createSchema({
  tables: [issue, issueLabel, label, owner, comment],
  relationships: [
    relationships(issue, ({many}) => ({
      issueLabels: many({
        sourceField: ['id'],
        destField: ['issueID'],
        destSchema: issueLabel,
      }),
      comments: many({
        sourceField: ['id'],
        destField: ['issueID'],
        destSchema: comment,
      }),
    })),
    relationships(issueLabel, ({many}) => ({
      label: many({
        sourceField: ['labelID'],
        destField: ['id'],
        destSchema: label,
      }),
    })),
    relationships(label, ({many}) => ({
      owner: many({
        sourceField: ['ownerID'],
        destField: ['id'],
        destSchema: owner,
      }),
    })),
    relationships(comment, ({many}) => ({
      author: many({
        sourceField: ['authorID'],
        destField: ['id'],
        destSchema: owner,
      }),
    })),
  ],
});

type Rows = Partial<Record<keyof typeof schema.tables, Row[]>>;

function makeSources(rows: Rows): Record<string, Source> {
  const sources: Record<string, Source> = {};
  for (const name of Object.keys(schema.tables) as (keyof Rows)[]) {
    const {columns, primaryKey} = schema.tables[name];
    const source = createSource(lc, testLogConfig, name, columns, primaryKey);
    for (const row of rows[name] ?? []) {
      consume(source.push(makeSourceChangeAdd(row)));
    }
    sources[name] = source;
  }
  return sources;
}

type Q = Query<'issue', typeof schema>;

interface StepResult {
  got: number[];
  want: number[];
  parentLevel: string[];
  allPushes: string[];
}

function runMutation(
  q: Q,
  before: Rows,
  mutate: (sources: Record<string, Source>) => void,
  after: Rows,
): StepResult {
  const viewSources = makeSources(before);
  const view = new QueryDelegateImpl({sources: viewSources}).materialize(q);

  const catchSources = makeSources(before);
  const caught = new QueryDelegateImpl({sources: catchSources}).materialize(
    q,
    (_query, input) => {
      const c = new Catch(input);
      c.fetch();
      return c;
    },
  );

  mutate(viewSources);
  mutate(catchSources);

  const fresh = new QueryDelegateImpl({
    sources: makeSources(after),
  }).materialize(q);

  const ids = (d: unknown) =>
    (d as {id: number}[]).map(r => r.id).sort((a, b) => a - b);
  return {
    got: ids(view.data),
    want: ids(fresh.data),
    parentLevel: caught.pushes
      .filter(c => c.type === 'add' || c.type === 'remove')
      .map(c =>
        c.node === 'yield'
          ? 'yield'
          : `${c.type}(${JSON.stringify(c.node.row.id)})`,
      ),
    allPushes: caught.pushes.map(c => {
      if (c.type === 'child') {
        return `child(${c.child.relationshipName})`;
      }
      if (c.type === 'edit') {
        return `edit(${JSON.stringify(c.row.id)})`;
      }
      return c.node === 'yield'
        ? 'yield'
        : `${c.type}(${JSON.stringify(c.node.row.id)})`;
    }),
  };
}

describe('Compound EXISTS / NOT EXISTS with AND / OR', () => {
  // Base data setup:
  // Issue 1:
  //   - comments 1, 2, 3 by author 1 ('bot')
  //   - comment 4 by author 2 ('bot')
  //   - Cap limit = 3 holds comments 1, 2, 3.
  //   - If author 1 is renamed to 'x', comments 1, 2, 3 drop out of Cap, and comment 4 refills.
  // Issue 2:
  //   - comment 5 by author 1 ('bot')
  //   - No other comments. If author 1 is renamed, comment 5 drops and no refill occurs.
  // Labels:
  //   - issueLabel 1 for issue 1 -> label 1 -> owner 3 ('active')
  //   - issueLabel 2 for issue 2 -> label 2 -> owner 3 ('active')

  describe('OR(EXISTS, EXISTS)', () => {
    // issue.where(({or, exists}) => or(
    //   exists('comments', c => c.whereExists('author', a => a.where('name', 'bot'))),
    //   exists('issueLabels', il => il.whereExists('label', l => l.whereExists('owner', o => o.where('name', 'labelBot'))))
    // ))
    const qOr = newQuery(schema, 'issue').where(({or, exists}) =>
      or(
        exists('comments', c =>
          c.whereExists('author', a => a.where('name', 'bot')),
        ),
        exists('issueLabels', il =>
          il.whereExists('label', l =>
            l.whereExists('owner', o => o.where('name', 'labelBot')),
          ),
        ),
      ),
    );

    test('branch A refills while branch B has no rows -> row stays, zero flicker', () => {
      const before: Rows = {
        issue: [{id: 1}],
        comment: [
          {id: 1, issueID: 1, authorID: 1},
          {id: 2, issueID: 1, authorID: 1},
          {id: 3, issueID: 1, authorID: 1},
          {id: 4, issueID: 1, authorID: 2}, // refill survivor
        ],
        owner: [
          {id: 1, name: 'bot'},
          {id: 2, name: 'bot'},
          {id: 3, name: 'other'},
        ],
        issueLabel: [],
        label: [],
      };
      const after: Rows = {
        ...before,
        owner: [
          {id: 1, name: 'x'},
          {id: 2, name: 'bot'},
          {id: 3, name: 'other'},
        ],
      };

      const result = runMutation(
        qOr,
        before,
        sources => {
          consume(
            must(sources.owner).push(
              makeSourceChangeEdit({id: 1, name: 'x'}, {id: 1, name: 'bot'}),
            ),
          );
        },
        after,
      );

      expect(result.got).toEqual(result.want);
      expect(result.want).toEqual([1]);
      // Crucial: issue 1 never left the view!
      expect(result.parentLevel).toEqual([]);
    });

    test('branch A drops to 0 (no refill) but branch B satisfies OR -> row stays, zero flicker', () => {
      // Issue 1:
      // comments: only comment 1 (by author 1) -> drops to 0, NO refill!
      // issueLabels: has label 1 owned by 'labelBot' -> satisfies branch B!
      const before: Rows = {
        issue: [{id: 1}],
        comment: [{id: 1, issueID: 1, authorID: 1}],
        issueLabel: [{id: 1, issueID: 1, labelID: 1}],
        label: [{id: 1, ownerID: 3}],
        owner: [
          {id: 1, name: 'bot'},
          {id: 3, name: 'labelBot'},
        ],
      };
      const after: Rows = {
        ...before,
        owner: [
          {id: 1, name: 'x'},
          {id: 3, name: 'labelBot'},
        ],
      };

      const result = runMutation(
        qOr,
        before,
        sources => {
          consume(
            must(sources.owner).push(
              makeSourceChangeEdit({id: 1, name: 'x'}, {id: 1, name: 'bot'}),
            ),
          );
        },
        after,
      );

      expect(result.got).toEqual(result.want);
      expect(result.want).toEqual([1]);
      // When branch A drops to 0 at reconcile, FilterStart flushes the deferred removal.
      // FanOut drives it to both branch A (false) and branch B (true).
      // FanIn evaluates OR(false, true) = true!
      // Therefore issue 1 MUST NOT be removed!
      expect(result.parentLevel).toEqual([]);
    });

    test('branch A drops to 0 (no refill) and branch B has no rows -> row removed cleanly at reconcile', () => {
      const before: Rows = {
        issue: [{id: 1}],
        comment: [{id: 1, issueID: 1, authorID: 1}], // no refill survivor
        issueLabel: [],
        label: [],
        owner: [{id: 1, name: 'bot'}],
      };
      const after: Rows = {
        ...before,
        owner: [{id: 1, name: 'x'}],
      };

      const result = runMutation(
        qOr,
        before,
        sources => {
          consume(
            must(sources.owner).push(
              makeSourceChangeEdit({id: 1, name: 'x'}, {id: 1, name: 'bot'}),
            ),
          );
        },
        after,
      );

      expect(result.got).toEqual(result.want);
      expect(result.want).toEqual([]);
      expect(result.parentLevel).toEqual(['remove(1)']);
    });

    test('BOTH branch A and branch B drop to 0 and both refill -> row stays, zero flicker', () => {
      // Both comments and labels have 3 items dropped and 1 survivor refilled
      const before: Rows = {
        issue: [{id: 1}],
        comment: [
          {id: 1, issueID: 1, authorID: 1},
          {id: 2, issueID: 1, authorID: 1},
          {id: 3, issueID: 1, authorID: 1},
          {id: 4, issueID: 1, authorID: 2}, // refill comment
        ],
        issueLabel: [
          {id: 1, issueID: 1, labelID: 1},
          {id: 2, issueID: 1, labelID: 1},
          {id: 3, issueID: 1, labelID: 1},
          {id: 4, issueID: 1, labelID: 2}, // refill label
        ],
        label: [
          {id: 1, ownerID: 1},
          {id: 2, ownerID: 2},
        ],
        owner: [
          {id: 1, name: 'bot'},
          {id: 2, name: 'labelBot'}, // both match their respective subqueries!
        ],
      };
      // Rename owner 1 away from 'bot'
      const after: Rows = {
        ...before,
        owner: [
          {id: 1, name: 'x'},
          {id: 2, name: 'labelBot'},
        ],
      };

      const result = runMutation(
        qOr,
        before,
        sources => {
          consume(
            must(sources.owner).push(
              makeSourceChangeEdit({id: 1, name: 'x'}, {id: 1, name: 'bot'}),
            ),
          );
        },
        after,
      );

      expect(result.got).toEqual(result.want);
      expect(result.want).toEqual([1]);
      expect(result.parentLevel).toEqual([]);
    });
  });

  describe('AND(EXISTS, EXISTS)', () => {
    // Both branches look for owner with name 'bot'
    const qAnd = newQuery(schema, 'issue').where(({and, exists}) =>
      and(
        exists('comments', c =>
          c.whereExists('author', a => a.where('name', 'bot')),
        ),
        exists('issueLabels', il =>
          il.whereExists('label', l =>
            l.whereExists('owner', o => o.where('name', 'bot')),
          ),
        ),
      ),
    );

    test('branch A drops to 0 and refills, branch B present -> row stays, zero flicker', () => {
      const before: Rows = {
        issue: [{id: 1}],
        comment: [
          {id: 1, issueID: 1, authorID: 1},
          {id: 2, issueID: 1, authorID: 1},
          {id: 3, issueID: 1, authorID: 1},
          {id: 4, issueID: 1, authorID: 2}, // refill
        ],
        issueLabel: [{id: 1, issueID: 1, labelID: 1}],
        label: [{id: 1, ownerID: 2}], // owned by owner 2 ('bot') -> unaffected!
        owner: [
          {id: 1, name: 'bot'},
          {id: 2, name: 'bot'},
        ],
      };
      const after: Rows = {
        ...before,
        owner: [
          {id: 1, name: 'x'},
          {id: 2, name: 'bot'},
        ],
      };

      const result = runMutation(
        qAnd,
        before,
        sources => {
          consume(
            must(sources.owner).push(
              makeSourceChangeEdit({id: 1, name: 'x'}, {id: 1, name: 'bot'}),
            ),
          );
        },
        after,
      );

      expect(result.got).toEqual(result.want);
      expect(result.want).toEqual([1]);
      expect(result.parentLevel).toEqual([]);
    });

    test('branch A refills, but branch B is removed in same push -> row removed', () => {
      const before: Rows = {
        issue: [{id: 1}],
        comment: [
          {id: 1, issueID: 1, authorID: 1},
          {id: 2, issueID: 1, authorID: 1},
          {id: 3, issueID: 1, authorID: 1},
          {id: 4, issueID: 1, authorID: 2}, // refill
        ],
        issueLabel: [{id: 1, issueID: 1, labelID: 1}],
        label: [{id: 1, ownerID: 1}], // label is owned by owner 1 ('bot')
        owner: [
          {id: 1, name: 'bot'},
          {id: 2, name: 'bot'},
        ],
      };
      // Renaming owner 1 from 'bot' to 'x' causes comments to refill (survivor author 2),
      // BUT label 1 owner changes to 'x', so issueLabels has NO survivors!
      const after: Rows = {
        ...before,
        owner: [
          {id: 1, name: 'x'},
          {id: 2, name: 'bot'},
        ],
      };

      const result = runMutation(
        qAnd,
        before,
        sources => {
          consume(
            must(sources.owner).push(
              makeSourceChangeEdit({id: 1, name: 'x'}, {id: 1, name: 'bot'}),
            ),
          );
        },
        after,
      );

      expect(result.got).toEqual(result.want);
      expect(result.want).toEqual([]);
      expect(result.parentLevel).toEqual(['remove(1)']);
    });
  });

  describe('AND(EXISTS, NOT EXISTS)', () => {
    // issue must have comments by 'bot', AND must NOT have any issueLabels
    const qExistsNotExists = newQuery(schema, 'issue').where(
      ({and, exists, not}) =>
        and(
          exists('comments', c =>
            c.whereExists('author', a => a.where('name', 'bot')),
          ),
          not(exists('issueLabels', il => il)),
        ),
    );

    test('EXISTS refills while NOT EXISTS remains empty -> row stays, zero flicker', () => {
      const before: Rows = {
        issue: [{id: 1}],
        comment: [
          {id: 1, issueID: 1, authorID: 1},
          {id: 2, issueID: 1, authorID: 1},
          {id: 3, issueID: 1, authorID: 1},
          {id: 4, issueID: 1, authorID: 2}, // refill
        ],
        issueLabel: [],
        owner: [
          {id: 1, name: 'bot'},
          {id: 2, name: 'bot'},
        ],
      };
      const after: Rows = {
        ...before,
        owner: [
          {id: 1, name: 'x'},
          {id: 2, name: 'bot'},
        ],
      };

      const result = runMutation(
        qExistsNotExists,
        before,
        sources => {
          consume(
            must(sources.owner).push(
              makeSourceChangeEdit({id: 1, name: 'x'}, {id: 1, name: 'bot'}),
            ),
          );
        },
        after,
      );

      expect(result.got).toEqual(result.want);
      expect(result.want).toEqual([1]);
      expect(result.parentLevel).toEqual([]);
    });

    test('EXISTS drops to 0 (no refill) -> row removed cleanly', () => {
      const before: Rows = {
        issue: [{id: 1}],
        comment: [{id: 1, issueID: 1, authorID: 1}], // no refill
        issueLabel: [],
        owner: [{id: 1, name: 'bot'}],
      };
      const after: Rows = {
        ...before,
        owner: [{id: 1, name: 'x'}],
      };

      const result = runMutation(
        qExistsNotExists,
        before,
        sources => {
          consume(
            must(sources.owner).push(
              makeSourceChangeEdit({id: 1, name: 'x'}, {id: 1, name: 'bot'}),
            ),
          );
        },
        after,
      );

      expect(result.got).toEqual(result.want);
      expect(result.want).toEqual([]);
      expect(result.parentLevel).toEqual(['remove(1)']);
    });
  });

  describe('NOT EXISTS standalone', () => {
    const qNotExists = newQuery(schema, 'issue').where(({not, exists}) =>
      not(
        exists('comments', c =>
          c.whereExists('author', a => a.where('name', 'bot')),
        ),
      ),
    );

    test('comments drop 1 -> 0 and refill -> row must NEVER flicker into view', () => {
      // Issue 1 currently has matching comments, so it is NOT in the view.
      // When comments drop 3 -> 0 in Phase 1, issue 1 must NOT momentarily enter the view!
      const before: Rows = {
        issue: [{id: 1}],
        comment: [
          {id: 1, issueID: 1, authorID: 1},
          {id: 2, issueID: 1, authorID: 1},
          {id: 3, issueID: 1, authorID: 1},
          {id: 4, issueID: 1, authorID: 2}, // refill
        ],
        owner: [
          {id: 1, name: 'bot'},
          {id: 2, name: 'bot'},
        ],
      };
      const after: Rows = {
        ...before,
        owner: [
          {id: 1, name: 'x'},
          {id: 2, name: 'bot'},
        ],
      };

      const result = runMutation(
        qNotExists,
        before,
        sources => {
          consume(
            must(sources.owner).push(
              makeSourceChangeEdit({id: 1, name: 'x'}, {id: 1, name: 'bot'}),
            ),
          );
        },
        after,
      );

      expect(result.got).toEqual(result.want);
      expect(result.want).toEqual([]);
      // Crucial: issue 1 was never added!
      expect(result.parentLevel).toEqual([]);
    });

    test('comments drop 1 -> 0 without refill -> row enters view cleanly at reconcile', () => {
      const before: Rows = {
        issue: [{id: 1}],
        comment: [{id: 1, issueID: 1, authorID: 1}],
        owner: [{id: 1, name: 'bot'}],
      };
      const after: Rows = {
        ...before,
        owner: [{id: 1, name: 'x'}],
      };

      const result = runMutation(
        qNotExists,
        before,
        sources => {
          consume(
            must(sources.owner).push(
              makeSourceChangeEdit({id: 1, name: 'x'}, {id: 1, name: 'bot'}),
            ),
          );
        },
        after,
      );

      expect(result.got).toEqual(result.want);
      expect(result.want).toEqual([1]);
      expect(result.parentLevel).toEqual(['add(1)']);
    });
  });

  describe('Multi-parent batch mutation', () => {
    // Mutation affects multiple issues at the same time:
    // Issue 1: comments refill (survives, no flicker)
    // Issue 2: comments do NOT refill (removed)
    // Issue 3: never had comments (stays out)
    const q = newQuery(schema, 'issue').whereExists('comments', c =>
      c.whereExists('author', a => a.where('name', 'bot')),
    );

    test('handles multiple parents with different refill outcomes in single push', () => {
      const before: Rows = {
        issue: [{id: 1}, {id: 2}, {id: 3}],
        comment: [
          // Issue 1: 3 doomed, 1 survivor
          {id: 1, issueID: 1, authorID: 1},
          {id: 2, issueID: 1, authorID: 1},
          {id: 3, issueID: 1, authorID: 1},
          {id: 4, issueID: 1, authorID: 2},
          // Issue 2: 1 doomed, 0 survivors
          {id: 5, issueID: 2, authorID: 1},
        ],
        owner: [
          {id: 1, name: 'bot'},
          {id: 2, name: 'bot'},
        ],
      };
      const after: Rows = {
        ...before,
        owner: [
          {id: 1, name: 'x'},
          {id: 2, name: 'bot'},
        ],
      };

      const result = runMutation(
        q,
        before,
        sources => {
          consume(
            must(sources.owner).push(
              makeSourceChangeEdit({id: 1, name: 'x'}, {id: 1, name: 'bot'}),
            ),
          );
        },
        after,
      );

      expect(result.got).toEqual(result.want);
      expect(result.want).toEqual([1]);
      // Issue 1 must NOT be in parentLevel (no remove/add)!
      // Only Issue 2 is removed.
      expect(result.parentLevel).toEqual(['remove(2)']);
    });

    test('opposed multi-change: Issue 1 refills, Issue 2 removed, Issue 3 added in same push', () => {
      // Issue 1: comments refill (survives, no flicker)
      // Issue 2: comments drop to 0 and does not refill (removed at reconcile)
      // Issue 3: previously had author 3 ('other'). Owner 3 is renamed to 'bot', so Issue 3 enters!
      const before: Rows = {
        issue: [{id: 1}, {id: 2}, {id: 3}],
        comment: [
          // Issue 1: 3 doomed by owner 1, 1 survivor by owner 2
          {id: 1, issueID: 1, authorID: 1},
          {id: 2, issueID: 1, authorID: 1},
          {id: 3, issueID: 1, authorID: 1},
          {id: 4, issueID: 1, authorID: 2},
          // Issue 2: 1 doomed by owner 1, 0 survivors
          {id: 5, issueID: 2, authorID: 1},
          // Issue 3: comment by owner 3 ('other')
          {id: 6, issueID: 3, authorID: 3},
        ],
        owner: [
          {id: 1, name: 'bot'},
          {id: 2, name: 'bot'},
          {id: 3, name: 'other'},
        ],
      };
      // Owner 1 renamed to 'x', Owner 3 renamed to 'bot'
      const after: Rows = {
        ...before,
        owner: [
          {id: 1, name: 'x'},
          {id: 2, name: 'bot'},
          {id: 3, name: 'bot'},
        ],
      };

      const result = runMutation(
        q,
        before,
        sources => {
          consume(
            must(sources.owner).push(
              makeSourceChangeEdit({id: 1, name: 'x'}, {id: 1, name: 'bot'}),
            ),
          );
          consume(
            must(sources.owner).push(
              makeSourceChangeEdit(
                {id: 3, name: 'bot'},
                {id: 3, name: 'other'},
              ),
            ),
          );
        },
        after,
      );

      expect(result.got).toEqual(result.want);
      expect(result.want).toEqual([1, 3]);
      // Issue 1 did NOT flicker!
      // Issue 2 was removed, Issue 3 was added.
      expect(result.parentLevel.sort()).toEqual(['add(3)', 'remove(2)'].sort());
    });
  });

  describe('OR(EXISTS, NOT EXISTS)', () => {
    // Matches if issue has comments by 'bot' OR has NO issueLabels
    const qOrNotExists = newQuery(schema, 'issue').where(({or, exists, not}) =>
      or(
        exists('comments', c =>
          c.whereExists('author', a => a.where('name', 'bot')),
        ),
        not(exists('issueLabels', il => il)),
      ),
    );

    test('EXISTS drops to 0 (no refill) but NOT EXISTS is true (no labels) -> row stays, zero flicker', () => {
      // Issue 1: comment drops to 0 (no refill), BUT issue has NO labels!
      // So NOT EXISTS is true, satisfying the OR!
      const before: Rows = {
        issue: [{id: 1}],
        comment: [{id: 1, issueID: 1, authorID: 1}],
        issueLabel: [],
        owner: [{id: 1, name: 'bot'}],
      };
      const after: Rows = {
        ...before,
        owner: [{id: 1, name: 'x'}],
      };

      const result = runMutation(
        qOrNotExists,
        before,
        sources => {
          consume(
            must(sources.owner).push(
              makeSourceChangeEdit({id: 1, name: 'x'}, {id: 1, name: 'bot'}),
            ),
          );
        },
        after,
      );

      expect(result.got).toEqual(result.want);
      expect(result.want).toEqual([1]);
      // Issue 1 never left the view!
      expect(result.parentLevel).toEqual([]);
    });

    test('EXISTS drops to 0 (no refill) and NOT EXISTS is false (has labels) -> row removed cleanly', () => {
      const before: Rows = {
        issue: [{id: 1}],
        comment: [{id: 1, issueID: 1, authorID: 1}],
        issueLabel: [{id: 1, issueID: 1, labelID: 1}],
        label: [{id: 1, ownerID: 1}],
        owner: [{id: 1, name: 'bot'}],
      };
      const after: Rows = {
        ...before,
        owner: [{id: 1, name: 'x'}],
      };

      const result = runMutation(
        qOrNotExists,
        before,
        sources => {
          consume(
            must(sources.owner).push(
              makeSourceChangeEdit({id: 1, name: 'x'}, {id: 1, name: 'bot'}),
            ),
          );
        },
        after,
      );

      expect(result.got).toEqual(result.want);
      expect(result.want).toEqual([]);
      expect(result.parentLevel).toEqual(['remove(1)']);
    });
  });

  describe('Nested condition: AND(OR(EXISTS(comments), EXISTS(labels)), EXISTS(comments_second))', () => {
    // issue must satisfy:
    // (comments by 'bot' OR labels by 'bot') AND comments by 'author2'
    const qNested = newQuery(schema, 'issue').where(({and, or, exists}) =>
      and(
        or(
          exists('comments', c =>
            c.whereExists('author', a => a.where('name', 'bot')),
          ),
          exists('issueLabels', il =>
            il.whereExists('label', l =>
              l.whereExists('owner', o => o.where('name', 'bot')),
            ),
          ),
        ),
        exists('comments', c =>
          c.whereExists('author', a => a.where('name', 'permanent')),
        ),
      ),
    );

    test('branch A refills, branch B empty, outer AND satisfied -> row stays, zero flicker', () => {
      const before: Rows = {
        issue: [{id: 1}],
        comment: [
          // 3 doomed by owner 1 ('bot'), 1 refill survivor by owner 2 ('bot')
          {id: 1, issueID: 1, authorID: 1},
          {id: 2, issueID: 1, authorID: 1},
          {id: 3, issueID: 1, authorID: 1},
          {id: 4, issueID: 1, authorID: 2},
          // Permanent comment by owner 3 ('permanent')
          {id: 5, issueID: 1, authorID: 3},
        ],
        issueLabel: [],
        label: [],
        owner: [
          {id: 1, name: 'bot'},
          {id: 2, name: 'bot'},
          {id: 3, name: 'permanent'},
        ],
      };
      const after: Rows = {
        ...before,
        owner: [
          {id: 1, name: 'x'},
          {id: 2, name: 'bot'},
          {id: 3, name: 'permanent'},
        ],
      };

      const result = runMutation(
        qNested,
        before,
        sources => {
          consume(
            must(sources.owner).push(
              makeSourceChangeEdit({id: 1, name: 'x'}, {id: 1, name: 'bot'}),
            ),
          );
        },
        after,
      );

      expect(result.got).toEqual(result.want);
      expect(result.want).toEqual([1]);
      expect(result.parentLevel).toEqual([]);
    });

    test('branch A refills, branch B empty, BUT outer AND condition drops to 0 -> row removed cleanly', () => {
      // Owner 3 ('permanent') is renamed away, so outer AND is no longer satisfied!
      const before: Rows = {
        issue: [{id: 1}],
        comment: [
          {id: 1, issueID: 1, authorID: 1},
          {id: 2, issueID: 1, authorID: 1},
          {id: 3, issueID: 1, authorID: 1},
          {id: 4, issueID: 1, authorID: 2},
          {id: 5, issueID: 1, authorID: 3},
        ],
        issueLabel: [],
        label: [],
        owner: [
          {id: 1, name: 'bot'},
          {id: 2, name: 'bot'},
          {id: 3, name: 'permanent'},
        ],
      };
      const after: Rows = {
        ...before,
        owner: [
          {id: 1, name: 'x'},
          {id: 2, name: 'bot'},
          {id: 3, name: 'x'},
        ],
      };

      const result = runMutation(
        qNested,
        before,
        sources => {
          consume(
            must(sources.owner).push(
              makeSourceChangeEdit({id: 1, name: 'x'}, {id: 1, name: 'bot'}),
            ),
          );
          consume(
            must(sources.owner).push(
              makeSourceChangeEdit(
                {id: 3, name: 'x'},
                {id: 3, name: 'permanent'},
              ),
            ),
          );
        },
        after,
      );

      expect(result.got).toEqual(result.want);
      expect(result.want).toEqual([]);
      expect(result.parentLevel).toEqual(['remove(1)']);
    });
  });

  describe('Parent row edits during EXISTS refills', () => {
    const q = newQuery(schema, 'issue').whereExists('comments', c =>
      c.whereExists('author', a => a.where('name', 'bot')),
    );

    test('parent non-PK column edit concurrent with refill -> edit emitted, zero remove/add flicker', () => {
      const before: Rows = {
        issue: [{id: 1, title: 'Old Title'}],
        comment: [
          {id: 1, issueID: 1, authorID: 1},
          {id: 2, issueID: 1, authorID: 1},
          {id: 3, issueID: 1, authorID: 1},
          {id: 4, issueID: 1, authorID: 2}, // refill
        ],
        owner: [
          {id: 1, name: 'bot'},
          {id: 2, name: 'bot'},
        ],
      };
      // Issue 1 title is edited AND author 1 is renamed to 'x'
      const after: Rows = {
        ...before,
        issue: [{id: 1, title: 'New Title'}],
        owner: [
          {id: 1, name: 'x'},
          {id: 2, name: 'bot'},
        ],
      };

      const result = runMutation(
        q,
        before,
        sources => {
          consume(
            must(sources.issue).push(
              makeSourceChangeEdit(
                {id: 1, title: 'New Title'},
                {id: 1, title: 'Old Title'},
              ),
            ),
          );
          consume(
            must(sources.owner).push(
              makeSourceChangeEdit({id: 1, name: 'x'}, {id: 1, name: 'bot'}),
            ),
          );
        },
        after,
      );

      expect(result.got).toEqual(result.want);
      expect(result.want).toEqual([1]);
      // Zero remove/add flicker!
      expect(result.parentLevel).toEqual([]);
      // An edit was emitted for issue 1!
      expect(result.allPushes).toContain('edit(1)');
    });
  });
});
