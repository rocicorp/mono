import {en, Faker, generateMersenne53Randomizer} from '@faker-js/faker';
import {expect, test} from 'vitest';
import {type AST} from '../../zero-protocol/src/ast.ts';
import {ast} from '../../zql/src/query/query-impl.ts';
import {staticQuery} from '../../zql/src/query/static-query.ts';
import {generateQuery} from '../../zql/src/query/test/query-gen.ts';
import {generateSchema} from '../../zql/src/query/test/schema-gen.ts';
import {astToZQL} from './ast-to-zql.ts';

test('simple table selection', () => {
  const ast: AST = {
    table: 'issue',
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(`""`);
});

test('simple where condition with equality', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'simple',
      left: {type: 'column', name: 'id'},
      op: '=',
      right: {type: 'literal', value: 123},
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(`".where('id', 123)"`);
});

test('where condition with non-equality operator', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'simple',
      left: {type: 'column', name: 'priority'},
      op: '>',
      right: {type: 'literal', value: 2},
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(`".where('priority', '>', 2)"`);
});

test('simple where condition with single AND', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          left: {type: 'column', name: 'id'},
          op: '=',
          right: {type: 'literal', value: 123},
        },
      ],
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(`".where('id', 123)"`);
});

test('simple where condition with single OR', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'or',
      conditions: [
        {
          type: 'simple',
          left: {type: 'column', name: 'id'},
          op: '=',
          right: {type: 'literal', value: 123},
        },
      ],
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(`".where('id', 123)"`);
});

test('AND condition using multiple where clauses', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          left: {type: 'column', name: 'id'},
          op: '=',
          right: {type: 'literal', value: 123},
        },
        {
          type: 'simple',
          left: {type: 'column', name: 'status'},
          op: '=',
          right: {type: 'literal', value: 'open'},
        },
      ],
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".where('id', 123).where('status', 'open')"`,
  );
});

test('only top level AND should be spread into where calls', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          left: {type: 'column', name: 'id'},
          op: '=',
          right: {type: 'literal', value: 123},
        },
        {
          type: 'or',
          conditions: [
            {
              type: 'simple',
              left: {type: 'column', name: 'status'},
              op: '=',
              right: {type: 'literal', value: 'open'},
            },
            {
              type: 'and',
              conditions: [
                {
                  type: 'simple',
                  left: {type: 'column', name: 'status'},
                  op: '=',
                  right: {type: 'literal', value: 'in-progress'},
                },
                {
                  type: 'simple',
                  left: {type: 'column', name: 'priority'},
                  op: '>=',
                  right: {type: 'literal', value: 3},
                },
              ],
            },
          ],
        },
        {
          type: 'simple',
          left: {type: 'column', name: 'status'},
          op: '=',
          right: {type: 'literal', value: 'open'},
        },
      ],
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".where('id', 123).where(({and, cmp, or}) => or(cmp('status', 'open'), and(cmp('status', 'in-progress'), cmp('priority', '>=', 3)))).where('status', 'open')"`,
  );
});

test('OR condition', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'or',
      conditions: [
        {
          type: 'simple',
          left: {type: 'column', name: 'status'},
          op: '=',
          right: {type: 'literal', value: 'open'},
        },
        {
          type: 'simple',
          left: {type: 'column', name: 'status'},
          op: '=',
          right: {type: 'literal', value: 'in-progress'},
        },
      ],
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".where(({cmp, or}) => or(cmp('status', 'open'), cmp('status', 'in-progress')))"`,
  );
});

test('with orderBy', () => {
  const ast: AST = {
    table: 'issue',
    orderBy: [
      ['priority', 'desc'],
      ['created_at', 'asc'],
    ],
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".orderBy('priority', 'desc').orderBy('created_at', 'asc')"`,
  );
});

test('with limit', () => {
  const ast: AST = {
    table: 'issue',
    limit: 10,
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(`".limit(10)"`);
});

test('with start', () => {
  const ast: AST = {
    table: 'issue',
    start: {
      row: {id: 5},
      exclusive: false,
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".start({"id":5}, { inclusive: true })"`,
  );
});

test('whereExists condition', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      related: {
        correlation: {
          parentField: ['id'],
          childField: ['issue_id'],
        },
        subquery: {
          table: 'comment',
          alias: 'zsubq_comments',
        },
      },
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(`".whereExists('comments')"`);
});

test('whereNotExists condition', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'correlatedSubquery',
      op: 'NOT EXISTS',
      related: {
        correlation: {
          parentField: ['id'],
          childField: ['issue_id'],
        },
        subquery: {
          table: 'comment',
          alias: 'zsubq_comments',
        },
      },
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".where(({exists, not}) => not(exists('comments')))"`,
  );
});

test('whereNotExists condition with orderBy in subquery', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'correlatedSubquery',
      op: 'NOT EXISTS',
      related: {
        correlation: {
          parentField: ['id'],
          childField: ['issue_id'],
        },
        subquery: {
          table: 'comment',
          alias: 'zsubq_comments',
          orderBy: [['created_at', 'desc']],
        },
      },
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".where(({exists, not}) => not(exists('comments', q => q.orderBy('created_at', 'desc'))))"`,
  );
});

test('NOT LIKE operator', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'simple',
      left: {type: 'column', name: 'title'},
      op: 'NOT LIKE',
      right: {type: 'literal', value: '%urgent%'},
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".where('title', 'NOT LIKE', '%urgent%')"`,
  );
});

test('NOT ILIKE operator', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'simple',
      left: {type: 'column', name: 'title'},
      op: 'NOT ILIKE',
      right: {type: 'literal', value: '%urgent%'},
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".where('title', 'NOT ILIKE', '%urgent%')"`,
  );
});

test('NOT LIKE in complex condition', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          left: {type: 'column', name: 'title'},
          op: 'NOT LIKE',
          right: {type: 'literal', value: '%bug%'},
        },
        {
          type: 'simple',
          left: {type: 'column', name: 'status'},
          op: '=',
          right: {type: 'literal', value: 'open'},
        },
      ],
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".where('title', 'NOT LIKE', '%bug%').where('status', 'open')"`,
  );
});

test('related query', () => {
  const ast: AST = {
    table: 'issue',
    related: [
      {
        correlation: {
          parentField: ['id'],
          childField: ['issue_id'],
        },
        subquery: {
          table: 'comment',
          alias: 'comments',
        },
      },
    ],
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(`".related('comments')"`);
});

test('related query with filters', () => {
  const ast: AST = {
    table: 'issue',
    related: [
      {
        correlation: {
          parentField: ['id'],
          childField: ['issue_id'],
        },
        subquery: {
          table: 'comment',
          alias: 'comments',
          where: {
            type: 'simple',
            left: {type: 'column', name: 'is_deleted'},
            op: '=',
            right: {type: 'literal', value: false},
          },
        },
      },
    ],
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".related('comments', q => q.where('is_deleted', false))"`,
  );
});

test('complex query with multiple features', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          left: {type: 'column', name: 'status'},
          op: '!=',
          right: {type: 'literal', value: 'closed'},
        },
        {
          type: 'simple',
          left: {type: 'column', name: 'priority'},
          op: '>=',
          right: {type: 'literal', value: 3},
        },
      ],
    },
    orderBy: [['created_at', 'desc']],
    limit: 20,
    related: [
      {
        correlation: {
          parentField: ['id'],
          childField: ['issue_id'],
        },
        subquery: {
          table: 'comment',
          alias: 'comments',
          limit: 5,
          orderBy: [['created_at', 'desc']],
        },
      },
    ],
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".where('status', '!=', 'closed').where('priority', '>=', 3).related('comments', q => q.orderBy('created_at', 'desc').limit(5)).orderBy('created_at', 'desc').limit(20)"`,
  );
});

test('with auth parameter', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'simple',
      left: {type: 'column', name: 'owner_id'},
      op: '=',
      right: {
        type: 'static',
        anchor: 'authData',
        field: 'id',
      },
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".where('owner_id', authParam('id'))"`,
  );
});

test('EXISTS with order', () => {
  const ast: AST = {
    table: 'users',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      related: {
        correlation: {parentField: ['recruiterID'], childField: ['id']},
        subquery: {
          table: 'users',
          alias: 'zsubq_recruiter',
          where: {
            type: 'simple',
            left: {type: 'column', name: 'y'},
            op: '>',
            right: {type: 'literal', value: 0},
          },
        },
      },
      op: 'EXISTS',
    },
  };

  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".whereExists('recruiter', q => q.where('y', '>', 0)).orderBy('id', 'asc')"`,
  );
});

test('round trip', () => {
  const randomizer = generateMersenne53Randomizer(42);
  const rng = () => randomizer.next();
  const faker = new Faker({
    locale: en,
    randomizer,
  });

  const codes: string[] = [];

  for (let i = 0; i < 10; i++) {
    const schema = generateSchema(rng, faker, 10);
    const q = generateQuery(schema, {}, rng, faker);

    const code = astToZQL(ast(q));
    codes.push(code);

    const q2 = new Function(
      'staticQuery',
      'schema',
      'tableName',
      `return staticQuery(schema, tableName)${code}`,
    )(staticQuery, schema, ast(q).table);
    expect(ast(q2)).toEqual(ast(q));
  }

  expect(codes).toMatchInlineSnapshot(`
    [
      ".where('nudge', 'IS NOT', false)",
      ".where('sanity', '>=', 0.34825546702330035).orderBy('harp', 'asc').orderBy('requirement', 'asc').orderBy('gastropod', 'desc').orderBy('word', 'asc')",
      ".orderBy('rim', 'asc').limit(8556)",
      ".where('cutlet', 'IS NOT', null).where('diversity', 'LIKE', 'non coniuratio quas').where('reach', 'ILIKE', 'tamquam aperio tempora').where('reach', 'tepesco statua decumbo').orderBy('alligator', 'desc').orderBy('kinase', 'asc').limit(1510)",
      ".where('tuba', 'IS NOT', 'pecus teneo torqueo').orderBy('atrium', 'asc').orderBy('tuba', 'desc').orderBy('alligator', 'asc').orderBy('maintainer', 'desc').orderBy('produce', 'asc')",
      ".where('cycle', '!=', 'sint circumvenio totam').orderBy('cycle', 'desc').orderBy('pear', 'desc').orderBy('bar', 'desc').orderBy('circumference', 'asc').orderBy('offset', 'desc').orderBy('in-joke', 'desc').limit(431)",
      ".where('information', 'LIKE', 'vaco cursus ascisco').orderBy('membership', 'desc')",
      ".where('futon', 'IS', 'cuius vulgo utrum').orderBy('casement', 'asc').orderBy('reservation', 'asc').orderBy('futon', 'desc')",
      ".where('desk', 'carmen rerum sophismata').where('hyphenation', 'IS', true).where('knuckle', '>', 5713840145639807).where('hyphenation', '!=', true).orderBy('pliers', 'asc').orderBy('pronoun', 'desc').orderBy('monster', 'asc')",
      ".where('monasticism', 'ILIKE', 'caute damno supra').orderBy('scaffold', 'asc').limit(913)",
    ]
  `);
});
