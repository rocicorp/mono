import {beforeAll, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {
  AST,
  Condition,
  ScalarSubqueryCondition,
} from '../../../../zero-protocol/src/ast.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {computeZqlSpecs} from '../../db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../db/specs.ts';
import {
  conditionToSQL,
  extractLiteralEqualityConstraints,
  isSimpleSubquery,
  resolveSimpleScalarSubqueries,
} from './resolve-scalar-subqueries.ts';

describe('resolve-scalar-subqueries', () => {
  let db: Database;
  const tableSpecs = new Map<string, LiteAndZqlSpec>();

  beforeAll(() => {
    const lc = createSilentLogContext();
    db = new Database(lc, ':memory:');
    db.exec(/* sql */ `
      CREATE TABLE users (
        id "text|NOT_NULL",
        email "text|NOT_NULL",
        name TEXT,
        _0_version TEXT NOT NULL
      );
      CREATE UNIQUE INDEX users_pkey ON users (id ASC);
      CREATE UNIQUE INDEX users_email_key ON users (email ASC);

      CREATE TABLE issues (
        id "text|NOT_NULL",
        "ownerId" TEXT,
        title TEXT,
        _0_version TEXT NOT NULL
      );
      CREATE UNIQUE INDEX issues_pkey ON issues (id ASC);

      CREATE TABLE "compoundKey" (
        a "text|NOT_NULL",
        b "text|NOT_NULL",
        val TEXT,
        _0_version TEXT NOT NULL
      );
      CREATE UNIQUE INDEX compound_pkey ON "compoundKey" (a ASC, b ASC);

      CREATE TABLE "noUnique" (
        id TEXT,
        val TEXT,
        _0_version TEXT NOT NULL
      );
      CREATE INDEX no_unique_idx ON "noUnique" (id ASC);
    `);

    // Insert test data
    db.exec(/* sql */ `
      INSERT INTO users (id, email, name, _0_version)
      VALUES ('u1', 'alice@example.com', 'Alice', '1');
      INSERT INTO users (id, email, name, _0_version)
      VALUES ('u2', 'bob@example.com', 'Bob', '1');

      INSERT INTO issues (id, "ownerId", title, _0_version)
      VALUES ('i1', 'u1', 'Bug report', '1');

      INSERT INTO "compoundKey" (a, b, val, _0_version)
      VALUES ('x', 'y', 'found', '1');
    `);

    computeZqlSpecs(lc, db, {includeBackfillingColumns: false}, tableSpecs);
  });

  describe('extractLiteralEqualityConstraints', () => {
    test('extracts from simple equality', () => {
      const condition: Condition = {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'email'},
        right: {type: 'literal', value: 'alice@example.com'},
      };
      const result = extractLiteralEqualityConstraints(condition);
      expect(result).toEqual(new Map([['email', 'alice@example.com']]));
    });

    test('ignores non-equality operators', () => {
      const condition: Condition = {
        type: 'simple',
        op: '>',
        left: {type: 'column', name: 'age'},
        right: {type: 'literal', value: 18},
      };
      const result = extractLiteralEqualityConstraints(condition);
      expect(result.size).toBe(0);
    });

    test('extracts from AND conjunction', () => {
      const condition: Condition = {
        type: 'and',
        conditions: [
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'a'},
            right: {type: 'literal', value: 'x'},
          },
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'b'},
            right: {type: 'literal', value: 'y'},
          },
        ],
      };
      const result = extractLiteralEqualityConstraints(condition);
      expect(result).toEqual(
        new Map([
          ['a', 'x'],
          ['b', 'y'],
        ]),
      );
    });

    test('does not extract from OR disjunction', () => {
      const condition: Condition = {
        type: 'or',
        conditions: [
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'email'},
            right: {type: 'literal', value: 'a@b.com'},
          },
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'email'},
            right: {type: 'literal', value: 'c@d.com'},
          },
        ],
      };
      const result = extractLiteralEqualityConstraints(condition);
      expect(result.size).toBe(0);
    });

    test('ignores column = column comparisons', () => {
      const condition: Condition = {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'a'},
        // right must be non-column per AST type, but static params are not literal
        right: {type: 'static', anchor: 'authData', field: 'sub'},
      };
      const result = extractLiteralEqualityConstraints(condition);
      expect(result.size).toBe(0);
    });
  });

  describe('isSimpleSubquery', () => {
    test('simple: unique key fully constrained by literal', () => {
      const subquery: AST = {
        table: 'users',
        where: {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'email'},
          right: {type: 'literal', value: 'alice@example.com'},
        },
      };
      expect(isSimpleSubquery(subquery, tableSpecs)).toBe(true);
    });

    test('simple: primary key constrained', () => {
      const subquery: AST = {
        table: 'users',
        where: {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'id'},
          right: {type: 'literal', value: 'u1'},
        },
      };
      expect(isSimpleSubquery(subquery, tableSpecs)).toBe(true);
    });

    test('not simple: no unique index on table', () => {
      const subquery: AST = {
        table: 'noUnique',
        where: {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'id'},
          right: {type: 'literal', value: 'x'},
        },
      };
      // noUnique has no unique index, so tableSpecs won't have it
      // (computeZqlSpecs filters out tables without a primary key)
      expect(isSimpleSubquery(subquery, tableSpecs)).toBe(false);
    });

    test('not simple: partial unique key constrained', () => {
      const subquery: AST = {
        table: 'compoundKey',
        where: {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'a'},
          right: {type: 'literal', value: 'x'},
        },
      };
      // Only 'a' is constrained, but unique key requires both 'a' and 'b'
      expect(isSimpleSubquery(subquery, tableSpecs)).toBe(false);
    });

    test('simple: compound unique key fully constrained', () => {
      const subquery: AST = {
        table: 'compoundKey',
        where: {
          type: 'and',
          conditions: [
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'a'},
              right: {type: 'literal', value: 'x'},
            },
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'b'},
              right: {type: 'literal', value: 'y'},
            },
          ],
        },
      };
      expect(isSimpleSubquery(subquery, tableSpecs)).toBe(true);
    });

    test('not simple: constraint via OR', () => {
      const subquery: AST = {
        table: 'users',
        where: {
          type: 'or',
          conditions: [
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'email'},
              right: {type: 'literal', value: 'a@b.com'},
            },
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'email'},
              right: {type: 'literal', value: 'c@d.com'},
            },
          ],
        },
      };
      expect(isSimpleSubquery(subquery, tableSpecs)).toBe(false);
    });

    test('not simple: no where clause', () => {
      const subquery: AST = {
        table: 'users',
      };
      expect(isSimpleSubquery(subquery, tableSpecs)).toBe(false);
    });

    test('not simple: unknown table', () => {
      const subquery: AST = {
        table: 'nonexistent',
        where: {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'id'},
          right: {type: 'literal', value: '1'},
        },
      };
      expect(isSimpleSubquery(subquery, tableSpecs)).toBe(false);
    });
  });

  describe('conditionToSQL', () => {
    test('simple equality', () => {
      const params: (string | number | boolean | null)[] = [];
      const sql = conditionToSQL(
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'email'},
          right: {type: 'literal', value: 'alice@example.com'},
        },
        params,
      );
      expect(sql).toBe('"email" = ?');
      expect(params).toEqual(['alice@example.com']);
    });

    test('AND conjunction', () => {
      const params: (string | number | boolean | null)[] = [];
      const sql = conditionToSQL(
        {
          type: 'and',
          conditions: [
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'a'},
              right: {type: 'literal', value: 1},
            },
            {
              type: 'simple',
              op: '>',
              left: {type: 'column', name: 'b'},
              right: {type: 'literal', value: 2},
            },
          ],
        },
        params,
      );
      expect(sql).toBe('("a" = ?) AND ("b" > ?)');
      expect(params).toEqual([1, 2]);
    });

    test('OR disjunction', () => {
      const params: (string | number | boolean | null)[] = [];
      const sql = conditionToSQL(
        {
          type: 'or',
          conditions: [
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'x'},
              right: {type: 'literal', value: 'a'},
            },
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'x'},
              right: {type: 'literal', value: 'b'},
            },
          ],
        },
        params,
      );
      expect(sql).toBe('("x" = ?) OR ("x" = ?)');
      expect(params).toEqual(['a', 'b']);
    });

    test('NULL literal', () => {
      const params: (string | number | boolean | null)[] = [];
      const sql = conditionToSQL(
        {
          type: 'simple',
          op: 'IS',
          left: {type: 'column', name: 'val'},
          right: {type: 'literal', value: null},
        },
        params,
      );
      expect(sql).toBe('"val" IS NULL');
      expect(params).toEqual([]);
    });
  });

  describe('resolveSimpleScalarSubqueries', () => {
    test('resolves simple = scalar subquery', () => {
      const ast: AST = {
        table: 'issues',
        where: {
          type: 'scalarSubquery',
          op: '=',
          field: ['ownerId'],
          subquery: {
            table: 'users',
            alias: 'zsubq_scalar_users',
            where: {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'email'},
              right: {type: 'literal', value: 'alice@example.com'},
            },
          },
          column: ['id'],
        },
      };

      const result = resolveSimpleScalarSubqueries(ast, tableSpecs, db);
      expect(result.ast.where).toEqual({
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'ownerId'},
        right: {type: 'literal', value: 'u1'},
      });
      // Companion should contain the subquery AST
      expect(result.companions).toHaveLength(1);
      expect(result.companions[0].ast).toEqual({
        table: 'users',
        alias: 'zsubq_scalar_users',
        where: {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'email'},
          right: {type: 'literal', value: 'alice@example.com'},
        },
      });
    });

    test('resolves simple IS NOT scalar subquery', () => {
      const ast: AST = {
        table: 'issues',
        where: {
          type: 'scalarSubquery',
          op: 'IS NOT',
          field: ['ownerId'],
          subquery: {
            table: 'users',
            alias: 'zsubq_scalar_users',
            where: {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'email'},
              right: {type: 'literal', value: 'alice@example.com'},
            },
          },
          column: ['id'],
        },
      };

      const result = resolveSimpleScalarSubqueries(ast, tableSpecs, db);
      expect(result.ast.where).toEqual({
        type: 'simple',
        op: 'IS NOT',
        left: {type: 'column', name: 'ownerId'},
        right: {type: 'literal', value: 'u1'},
      });
    });

    test('returns always-false when no rows match', () => {
      const ast: AST = {
        table: 'issues',
        where: {
          type: 'scalarSubquery',
          op: '=',
          field: ['ownerId'],
          subquery: {
            table: 'users',
            alias: 'zsubq_scalar_users',
            where: {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'email'},
              right: {type: 'literal', value: 'nonexistent@example.com'},
            },
          },
          column: ['id'],
        },
      };

      const result = resolveSimpleScalarSubqueries(ast, tableSpecs, db);
      expect(result.ast.where).toEqual({
        type: 'simple',
        op: '=',
        left: {type: 'literal', value: 1},
        right: {type: 'literal', value: 0},
      });
    });

    test('returns always-false when column value is null', () => {
      // Insert a user with null name
      db.exec(/* sql */ `
        INSERT INTO users (id, email, name, _0_version)
        VALUES ('u3', 'null-name@example.com', NULL, '1');
      `);

      const ast: AST = {
        table: 'issues',
        where: {
          type: 'scalarSubquery',
          op: '=',
          field: ['ownerId'],
          subquery: {
            table: 'users',
            alias: 'zsubq_scalar_users',
            where: {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'email'},
              right: {type: 'literal', value: 'null-name@example.com'},
            },
          },
          column: ['name'],
        },
      };

      const result = resolveSimpleScalarSubqueries(ast, tableSpecs, db);
      expect(result.ast.where).toEqual({
        type: 'simple',
        op: '=',
        left: {type: 'literal', value: 1},
        right: {type: 'literal', value: 0},
      });
    });

    test('leaves non-simple scalar subquery unchanged', () => {
      const condition: ScalarSubqueryCondition = {
        type: 'scalarSubquery',
        op: '=',
        field: ['ownerId'],
        subquery: {
          table: 'users',
          alias: 'zsubq_scalar_users',
          where: {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'name'},
            // name has no unique index
            right: {type: 'literal', value: 'Alice'},
          },
        },
        column: ['id'],
      };
      const ast: AST = {
        table: 'issues',
        where: condition,
      };

      const result = resolveSimpleScalarSubqueries(ast, tableSpecs, db);
      expect(result.ast.where).toBe(condition);
    });

    test('leaves compound field scalar subquery unchanged', () => {
      const condition: ScalarSubqueryCondition = {
        type: 'scalarSubquery',
        op: '=',
        field: ['a', 'b'],
        subquery: {
          table: 'users',
          alias: 'zsubq_scalar_users',
          where: {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'email'},
            right: {type: 'literal', value: 'alice@example.com'},
          },
        },
        column: ['id', 'name'],
      };
      const ast: AST = {
        table: 'issues',
        where: condition,
      };

      const result = resolveSimpleScalarSubqueries(ast, tableSpecs, db);
      expect(result.ast.where).toBe(condition);
    });

    test('resolves scalar subquery nested in AND', () => {
      const ast: AST = {
        table: 'issues',
        where: {
          type: 'and',
          conditions: [
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'title'},
              right: {type: 'literal', value: 'Bug report'},
            },
            {
              type: 'scalarSubquery',
              op: '=',
              field: ['ownerId'],
              subquery: {
                table: 'users',
                alias: 'zsubq_scalar_users',
                where: {
                  type: 'simple',
                  op: '=',
                  left: {type: 'column', name: 'email'},
                  right: {type: 'literal', value: 'alice@example.com'},
                },
              },
              column: ['id'],
            },
          ],
        },
      };

      const result = resolveSimpleScalarSubqueries(ast, tableSpecs, db);
      expect(result.ast.where).toEqual({
        type: 'and',
        conditions: [
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'title'},
            right: {type: 'literal', value: 'Bug report'},
          },
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'ownerId'},
            right: {type: 'literal', value: 'u1'},
          },
        ],
      });
    });

    test('resolves scalar subquery nested in OR', () => {
      const ast: AST = {
        table: 'issues',
        where: {
          type: 'or',
          conditions: [
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'title'},
              right: {type: 'literal', value: 'Bug report'},
            },
            {
              type: 'scalarSubquery',
              op: '=',
              field: ['ownerId'],
              subquery: {
                table: 'users',
                alias: 'zsubq_scalar_users',
                where: {
                  type: 'simple',
                  op: '=',
                  left: {type: 'column', name: 'id'},
                  right: {type: 'literal', value: 'u2'},
                },
              },
              column: ['id'],
            },
          ],
        },
      };

      const result = resolveSimpleScalarSubqueries(ast, tableSpecs, db);
      expect(result.ast.where).toEqual({
        type: 'or',
        conditions: [
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'title'},
            right: {type: 'literal', value: 'Bug report'},
          },
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'ownerId'},
            right: {type: 'literal', value: 'u2'},
          },
        ],
      });
    });

    test('resolves scalar subquery with compound unique key', () => {
      const ast: AST = {
        table: 'issues',
        where: {
          type: 'scalarSubquery',
          op: '=',
          field: ['ownerId'],
          subquery: {
            table: 'compoundKey',
            alias: 'zsubq_scalar_compoundKey',
            where: {
              type: 'and',
              conditions: [
                {
                  type: 'simple',
                  op: '=',
                  left: {type: 'column', name: 'a'},
                  right: {type: 'literal', value: 'x'},
                },
                {
                  type: 'simple',
                  op: '=',
                  left: {type: 'column', name: 'b'},
                  right: {type: 'literal', value: 'y'},
                },
              ],
            },
          },
          column: ['val'],
        },
      };

      const result = resolveSimpleScalarSubqueries(ast, tableSpecs, db);
      expect(result.ast.where).toEqual({
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'ownerId'},
        right: {type: 'literal', value: 'found'},
      });
    });

    test('resolves scalar subqueries in related subqueries', () => {
      const ast: AST = {
        table: 'issues',
        related: [
          {
            correlation: {
              parentField: ['id'],
              childField: ['ownerId'],
            },
            subquery: {
              table: 'issues',
              alias: 'child_issues',
              where: {
                type: 'scalarSubquery',
                op: '=',
                field: ['ownerId'],
                subquery: {
                  table: 'users',
                  alias: 'zsubq_scalar_users',
                  where: {
                    type: 'simple',
                    op: '=',
                    left: {type: 'column', name: 'email'},
                    right: {type: 'literal', value: 'bob@example.com'},
                  },
                },
                column: ['id'],
              },
            },
          },
        ],
      };

      const result = resolveSimpleScalarSubqueries(ast, tableSpecs, db);
      expect(result.ast.related![0].subquery.where).toEqual({
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'ownerId'},
        right: {type: 'literal', value: 'u2'},
      });
    });

    test('preserves non-scalar conditions', () => {
      const condition: Condition = {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'title'},
        right: {type: 'literal', value: 'test'},
      };
      const ast: AST = {
        table: 'issues',
        where: condition,
      };

      const result = resolveSimpleScalarSubqueries(ast, tableSpecs, db);
      expect(result.ast.where).toBe(condition);
    });
  });
});
