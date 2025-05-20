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

test('nested related query with filters', () => {
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
          related: [
            {
              correlation: {
                parentField: ['authorID'],
                childField: ['id'],
              },
              subquery: {
                table: 'user',
                alias: 'author',
                where: {
                  type: 'simple',
                  left: {type: 'column', name: 'name'},
                  op: '=',
                  right: {type: 'literal', value: 'Bob'},
                },
              },
            },
          ],
        },
      },
    ],
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".related('comments', q => q.where('is_deleted', false).related('author', q => q.where('name', 'Bob')))"`,
  );
});

test('related query with hidden junction', () => {
  const ast: AST = {
    table: 'issue',
    related: [
      {
        correlation: {
          parentField: ['id'],
          childField: ['issueId'],
        },
        hidden: true,
        subquery: {
          table: 'issueLabel',
          alias: 'labels',
          related: [
            {
              correlation: {
                parentField: ['labelId'],
                childField: ['id'],
              },
              subquery: {
                table: 'label',
                alias: 'labels',
              },
            },
          ],
        },
      },
    ],
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(`".related('labels')"`);
});

test('related query with hidden junction with filters', () => {
  const ast: AST = {
    table: 'issue',
    related: [
      {
        correlation: {
          parentField: ['id'],
          childField: ['issueId'],
        },
        hidden: true,
        subquery: {
          table: 'issueLabel',
          alias: 'labels',
          related: [
            {
              correlation: {
                parentField: ['labelId'],
                childField: ['id'],
              },
              subquery: {
                table: 'label',
                alias: 'labels',
                where: {
                  type: 'simple',
                  left: {type: 'column', name: 'name'},
                  op: '=',
                  right: {type: 'literal', value: 'Bob'},
                },
              },
            },
          ],
        },
      },
    ],
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".related('labels', q => q.where('name', 'Bob'))"`,
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
      ".where('nudge', 'IS NOT', false).where('nudge', false).where('nudge', true).where('nudge', 'IS NOT', false).limit(161)",
      ".orderBy('minister', 'desc').limit(7)",
      ".where('jellyfish', '>', 5621833577626124).where('jellyfish', '>=', 0.7636057941597608).where('gallery', 'ILIKE', 'thesaurus currus acerbitas').where('gallery', 'IS NOT', 'apud truculenter vorax').orderBy('jellyfish', 'asc').limit(23)",
      ".where('hovercraft', 'pauci claro sub').where('mouser', 'IS', 945127024848024).orderBy('dress', 'asc').limit(103)",
      ".where('tray', 'LIKE', 'ago sumptus aequus').where('summary', 2670955066778355).where('alliance', 'IS', 1568237475464614).related('ceramics', q => q.whereExists('merit', q => q.where(({exists, not}) => not(exists('foodstuffs', q => q.whereExists('polyester', q => q.where('juggernaut', 'ILIKE', 'harum turbo eligendi').orderBy('exhaust', 'desc').orderBy('piglet', 'asc').orderBy('chapel', 'asc').orderBy('juggernaut', 'asc').limit(72)).orderBy('sunbeam', 'asc').orderBy('flat', 'desc').orderBy('providence', 'desc').orderBy('manner', 'asc').orderBy('birdbath', 'desc').orderBy('taxicab', 'asc').orderBy('sermon', 'desc').limit(175)))).whereExists('merit', q => q.where(({exists, not}) => not(exists('foodstuffs', q => q.where('sermon', 'IS NOT', 7764336202301908).where('manner', '<=', 0.17483862726041255).where('simple', 'IS', 'corona aegrotatio acies').orderBy('simple', 'asc').orderBy('sunbeam', 'asc').limit(43)))).whereExists('merit', q => q.orderBy('utilization', 'asc').limit(11)).where('utilization', '!=', 1281268407626690).where('utilization', '>', 0.06229179813006269).where('utilization', 'IS NOT', 0.30625362078661167).where('utilization', '>=', 7985820826764154).orderBy('utilization', 'asc')).where('utilization', 'IS', 0.011031264428647214).orderBy('utilization', 'asc').limit(3)).where(({exists, not}) => not(exists('polyester', q => q.orderBy('exhaust', 'desc').orderBy('allegation', 'asc').orderBy('piglet', 'asc').orderBy('juggernaut', 'asc').limit(147)))).related('merit', q => q.where('utilization', '!=', 2999414583125125).where('utilization', 'IS NOT', 0.7306505102392461).orderBy('utilization', 'asc').limit(1)).orderBy('recommendation', 'asc').orderBy('intent', 'desc').orderBy('colon', 'asc').orderBy('shark', 'desc').orderBy('pharmacopoeia', 'desc').orderBy('analogy', 'desc').orderBy('starboard', 'desc').limit(13)).limit(152)",
      ".orderBy('daughter', 'desc').orderBy('hunt', 'asc').orderBy('fibre', 'asc').orderBy('hovercraft', 'asc').limit(130)",
      ".where('tool', '!=', true).where('management', '>', 0.34727703903050755).where('appliance', 'quidem deporto cuius').orderBy('appliance', 'desc').orderBy('wallaby', 'asc').orderBy('management', 'desc').orderBy('e-mail', 'desc').orderBy('gloom', 'desc').orderBy('redesign', 'desc').orderBy('calculus', 'desc').orderBy('hexagon', 'asc').limit(24)",
      ".where('travel', 'LIKE', 'curto adamo denique').orderBy('travel', 'desc').orderBy('earth', 'asc').orderBy('settler', 'desc').limit(78)",
      ".orderBy('scaffold', 'asc').limit(86)",
      ".where(({exists, not}) => not(exists('mountain', q => q.whereExists('obedience', q => q.where('simple', '!=', 'celebrer a usus').where('expense', 'IS NOT', 'argentum defungo consequatur').where('swine', '!=', true).orderBy('swine', 'asc').orderBy('pneumonia', 'asc').limit(119)).where('transparency', 'IS', 'adaugeo bardus confugo').where('junior', 'IS NOT', 'desino admitto adeo').where('junior', 'IS', 'acidus deduco capio').where('knitting', 'LIKE', 'pecus cicuta atavus').orderBy('knitting', 'asc').limit(47)))).where('swanling', 'ILIKE', 'aeger accusantium tres').where('swanling', 'LIKE', 'conitor adaugeo vito').where('swanling', 'ILIKE', 'perferendis spiritus cometes').related('mountain', q => q.whereExists('obedience', q => q.where('expense', 'IS', 'creta calco patria').where('pneumonia', '>=', 0.9811179078679357).orderBy('simple', 'desc').orderBy('expense', 'asc').orderBy('calculus', 'desc').orderBy('digit', 'desc').orderBy('pneumonia', 'desc').orderBy('swine', 'asc').limit(55)).related('obedience', q => q.where('pneumonia', '!=', 0.9976282974021565).where('simple', 'acerbitas contra terra').where('calculus', '!=', 'campana accedo tendo').orderBy('swine', 'asc').limit(123)).orderBy('transparency', 'asc').orderBy('babushka', 'desc').orderBy('knitting', 'asc').limit(33)).related('councilman', q => q.whereExists('obedience', q => q.where('calculus', 'IS', 'nam charisma verus').orderBy('calculus', 'desc').orderBy('elevation', 'desc').orderBy('swine', 'asc').limit(172)).whereExists('detective', q => q.whereExists('providence', q => q.where('obligation', 'IS NOT', false).where('venom', 'IS', 'totidem ulterius vigilo').where('venom', 'IS', 'calculus delicate censura').where('venom', '!=', 'tempore volaticus decerno').orderBy('venom', 'asc').orderBy('obligation', 'asc').limit(65)).where(({exists, not}) => not(exists('councilman', q => q.where('meal', 'LIKE', 'tolero arca conicio').orderBy('polarisation', 'desc').orderBy('ad', 'desc').orderBy('ostrich', 'asc').orderBy('disappointment', 'desc').orderBy('puritan', 'asc').orderBy('meal', 'asc').orderBy('battle', 'asc')))).where('collaboration', 'IS', 6530274190124113).where('publicity', 'LIKE', 'unus combibo fugiat').orderBy('SUV', 'asc').orderBy('collaboration', 'asc').orderBy('publicity', 'desc').orderBy('deer', 'asc').orderBy('carboxyl', 'desc').limit(65)).where('ad', 'atque minus nihil').where('ostrich', '!=', 5959768847818546).orderBy('puritan', 'desc').orderBy('battle', 'asc').limit(114)).limit(176)",
    ]
  `);
});
