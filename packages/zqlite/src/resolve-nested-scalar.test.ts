import {expect, test} from 'vitest';
import type {AST, LiteralValue} from '../../zero-protocol/src/ast.ts';
import type {PrimaryKey} from '../../zero-protocol/src/primary-key.ts';
import {resolveSimpleScalarSubqueries} from './resolve-scalar-subqueries.ts';

/**
 * Nested scalar gates resolve bottom-up, and the outer gate's "is it pinned?"
 * test runs on the *already-resolved* subquery. So a gate that looks unpinned
 * syntactically can still be honored, because an inner scalar contributed the
 * missing key column as a literal.
 *
 * This is the zbugs list-query shape: `issueLabel -> label` pins only `name`,
 * but `label`'s unique key is `(projectID, name)`; the inner `label -> project`
 * gate — pinned on the unique `lowerCaseName` — rewrites to
 * `label.projectID = <literal>`, which completes the key.
 */
const specs = new Map<string, {tableSpec: {uniqueKeys: PrimaryKey[]}}>([
  ['label', {tableSpec: {uniqueKeys: [['id'], ['projectID', 'name']]}}],
  ['project', {tableSpec: {uniqueKeys: [['id'], ['lowerCaseName']]}}],
]);

const ast: AST = {
  table: 'issueLabel',
  where: {
    type: 'correlatedSubquery',
    op: 'EXISTS',
    scalar: true,
    related: {
      correlation: {parentField: ['labelID'], childField: ['id']},
      subquery: {
        table: 'label',
        alias: 'zsubq_label',
        where: {
          type: 'and',
          conditions: [
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'name'},
              right: {type: 'literal', value: 'bug'},
            },
            {
              type: 'correlatedSubquery',
              op: 'EXISTS',
              scalar: true,
              related: {
                correlation: {parentField: ['projectID'], childField: ['id']},
                subquery: {
                  table: 'project',
                  alias: 'zsubq_project',
                  where: {
                    type: 'simple',
                    op: '=',
                    left: {type: 'column', name: 'lowerCaseName'},
                    right: {type: 'literal', value: 'zero'},
                  },
                },
              },
            },
          ],
        },
      },
    },
  },
};

test("an inner scalar completes the outer gate's unique key", () => {
  const executed: string[] = [];
  const execute = (sub: AST, childField: string): LiteralValue | undefined => {
    executed.push(`${sub.table}.${childField}`);
    return sub.table === 'project' ? 'proj1' : 'label1';
  };

  const {ast: resolved, ignoredScalarHints} = resolveSimpleScalarSubqueries(
    ast,
    specs,
    execute,
  );

  // Innermost first, then the outer gate over the rewritten subquery.
  expect(executed).toEqual(['project.id', 'label.id']);
  // Nothing was declined: both gates were honored.
  expect(ignoredScalarHints).toEqual([]);
  // The whole chain collapsed to a single literal comparison.
  expect(resolved.where).toEqual({
    type: 'simple',
    op: '=',
    left: {type: 'column', name: 'labelID'},
    right: {type: 'literal', value: 'label1'},
  });
});
