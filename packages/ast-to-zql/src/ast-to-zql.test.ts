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

test('not exists over a junction edge', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      op: 'NOT EXISTS',
      related: {
        correlation: {
          childField: ['issueId'],
          parentField: ['id'],
        },
        subquery: {
          alias: 'zsubq_labels',
          orderBy: [
            ['issueId', 'asc'],
            ['labelId', 'asc'],
          ],
          table: 'issueLabel',
          where: {
            op: 'EXISTS',
            related: {
              correlation: {
                childField: ['id'],
                parentField: ['labelId'],
              },
              subquery: {
                alias: 'zsubq_zhidden_labels',
                orderBy: [['id', 'asc']],
                table: 'label',
              },
              system: 'permissions',
            },
            type: 'correlatedSubquery',
          },
        },
        system: 'permissions',
      },
      type: 'correlatedSubquery',
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".where(({exists, not}) => not(exists('labels', q => q.orderBy('id', 'asc'))))"`,
  );
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
      "",
      ".limit(189)",
      ".where('diversity', 'IS', false).related('honesty', q => q.where(({exists, not}) => not(exists('character', q => q.where('legend', '!=', 'arcus custodia villa').where('hello', 'IS NOT', 'vorago cunabula varius').where('toaster', '<', 7785446983784807).orderBy('complication', 'desc').limit(26)))).orderBy('solvency', 'asc').orderBy('starboard', 'desc').orderBy('angle', 'asc').orderBy('stump', 'desc').orderBy('intent', 'asc').limit(91))",
      ".limit(84)",
      ".whereExists('bowling', q => q.where('fellow', 'IS NOT', 'repellat temptatio artificiose').where('fellow', 'vulgaris alveus cuius').orderBy('fellow', 'asc').orderBy('decryption', 'asc').limit(111)).where('hyphenation', '!=', 'iste abscido temptatio').where('hoof', 'LIKE', 'vitae corona compello').where('hoof', '!=', 'vulnus corona deduco').orderBy('hoof', 'asc').orderBy('corral', 'desc').limit(158)",
      ".where('cinema', 'LIKE', 'vapulus audax aeternus').where('nucleotidase', 'IS', 0.9954375161913099).where('cinema', 'IS NOT', 'tripudio patruus amplus').where('vol', 'ILIKE', 'utroque mollitia ea').orderBy('vol', 'desc').limit(121)",
      ".where('sesame', 'viscus desidero damnatio').where('flame', true).where('flame', 'IS NOT', false).related('mousse', q => q.where(({exists, not}) => not(exists('mousse', q => q.where('flame', 'IS', true).where('sesame', 'suffragium advenio tumultus').where('flame', true).orderBy('flame', 'asc').limit(54)))).where('flame', 'IS NOT', true).where('sesame', 'IS', 'alii stips depulso').where('flame', 'IS', true).where('flame', 'IS', false).related('mousse', q => q.whereExists('mousse', q => q.where(({exists, not}) => not(exists('mousse', q => q.whereExists('mousse', q => q.where('flame', 'IS NOT', true).orderBy('flame', 'asc')).where('sesame', 'LIKE', 'accedo taceo trucido').where('sesame', 'tenus solus tener').where('sesame', 'IS NOT', 'depopulo aetas cumque').where('flame', true).orderBy('flame', 'asc').limit(14)))).where('sesame', '!=', 'desino thalassinus suffragium').where('flame', true).where('flame', '!=', false).where('flame', 'IS NOT', true).orderBy('flame', 'asc').limit(195)).where('flame', 'IS', false).where('sesame', 'IS', 'quibusdam aequitas statim').where('flame', 'IS', false).orderBy('flame', 'asc').limit(28)).orderBy('flame', 'asc').limit(140)).limit(131)",
      ".limit(87)",
      ".where(({exists, not}) => not(exists('stump', q => q.where('utilization', 'IS', 0.6195262000363033).where('utilization', '>=', 0.5809503798881793).where('utilization', 'IS', 0.3892304716917885).orderBy('utilization', 'asc').limit(185)))).where('impostor', '>=', 0.9811179078679357).where('impostor', 'IS', 0.053347996988897695).where('impostor', '!=', 8482007360421777).where('impostor', '>=', 0.6486389567688156).related('stump', q => q.whereExists('publicity', q => q.where('impostor', 6739041575015569).where('impostor', 'IS', 363447937618870).orderBy('impostor', 'asc')).related('publicity', q => q.where(({exists, not}) => not(exists('stump', q => q.where(({exists, not}) => not(exists('publicity', q => q.where('impostor', '<', 0.5070596467387183).orderBy('impostor', 'asc')))).where('utilization', 'IS', 0.27194603941844875).where('utilization', '>', 0.3245118001753421).where('utilization', '<', 5362015133206958).orderBy('utilization', 'asc').limit(162)))).where('impostor', '<', 0.3683472137943513).where('impostor', 7919001435752841).where('impostor', '>=', 0.32302734575409386).related('stump', q => q.where(({exists, not}) => not(exists('publicity', q => q.where(({exists, not}) => not(exists('stump', q => q.where('utilization', 6509842943338541).orderBy('utilization', 'asc').limit(1)))).where('impostor', '<=', 0.7728095732903374).orderBy('impostor', 'asc')))).where('utilization', 'IS', 0.5180080664046248).where('utilization', '!=', 0.1698810060507353).where('utilization', 'IS NOT', 0.7587153211639207).related('publicity', q => q.where(({exists, not}) => not(exists('stump', q => q.where('utilization', 'IS NOT', 7902470347604776).orderBy('utilization', 'asc').limit(193)))).orderBy('impostor', 'asc').limit(1)).orderBy('utilization', 'asc').limit(177)).orderBy('impostor', 'asc').limit(65)).orderBy('utilization', 'asc').limit(113))",
    ]
  `);
});
