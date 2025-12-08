import {en, Faker, generateMersenne53Randomizer} from '@faker-js/faker';
import {expect, test} from 'vitest';
import {type AST} from '../../zero-protocol/src/ast.ts';
import type {AnyQueryBuilder} from '../../zql/src/query/query-builder.ts';
import {asQueryInternals} from '../../zql/src/query/query-internals.ts';
import {staticQueryBuilder} from '../../zql/src/query/static-query.ts';
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
      `return staticQueryBuilder(schema, tableName)${code}`,
    )(staticQueryBuilder, schema, ast(q).table);
    expect(ast(q2)).toEqual(ast(q));
  }

  expect(codes).toMatchInlineSnapshot(`
    [
      ".where('nudge', 'IS NOT', false).where('nudge', false).where('nudge', true).where('nudge', 'IS NOT', false).limit(161)",
      "",
      ".limit(189)",
      ".where(({exists, not}) => not(exists('honesty', q => q.where(({exists, not}) => not(exists('character', q => q.where('complication', '>=', 4100258587552683).where('complication', 0.29166257870930323)))).where('case', 'IS NOT', true).where('stump', '<', 5567851869464033).where('defendant', 'IS NOT', 'compello eaque alias')))).where('diversity', false).related('honesty', q => q.where(({exists, not}) => not(exists('character', q => q.where('legend', '!=', 'arcus custodia villa').where('hello', 'IS NOT', 'vorago cunabula varius').where('toaster', '<', 7785446983784807)))).where('best-seller', '<', 0.770921844644052).where('case', 'IS NOT', true).limit(123)).limit(40)",
      ".whereExists('ownership', q => q.where('mixture', '<=', 0.17483862726041255).where('honesty', 'IS', 'acies appositus vix')).where('allegation', '>=', 0.2160381825095996).where('exhaust', '!=', 7032475176166046).related('ownership', q => q.whereExists('printer', q => q.where(({exists, not}) => not(exists('maintainer', q => q.where('exhaust', '<', 4826306720255529).where('allegation', '<=', 0.7954499607897292)))).where('vista', 'IS', 'vestigium harum turbo').where('conservation', '>=', 7440555743576231).where('stitcher', 'IS', true).where('conservation', 3575691624737434)).where('mixture', '<=', 0.2085105143573358).related('printer', q => q.whereExists('maintainer', q => q.where(({exists, not}) => not(exists('ownership'))).where('exhaust', '>=', 5354566803250054).where('allegation', 'IS NOT', 3856879252438377).where('exhaust', '!=', 937736356344409), {flip: true}).where('conservation', '>', null).where('stitcher', true).where('vista', 'IS NOT', 'canis harum utrum').limit(64)).orderBy('mixture', 'asc').orderBy('honesty', 'asc').orderBy('suitcase', 'asc'))",
      ".limit(147)",
      ".whereExists('zebra', q => q.where(({exists, not}) => not(exists('godparent', q => q.where(({exists, not}) => not(exists('ownership', q => q.where('following', 'IS NOT', true).where('nucleotidase', '<', 2434174811155864)))).where('overheard', '!=', true).where('overheard', 'IS', true).where('swine', 'IS NOT', 0.20131210782243325).where('pigpen', 'illo calcar vobis')))).where('airport', '>=', 0.1248208139668483).where('airport', '>', 4167411365131850).where('airmail', '<=', 2574109428349849).where('contrail', 'IS NOT', true)).where('impostor', 'ILIKE', 'tendo nam viscus').where('impostor', 'uberrime varietas advenio').related('zebra', q => q.whereExists('godparent', q => q.whereExists('pantyhose', q => q.where('chairperson', 'IS NOT', null).where('peony', '!=', 'neque adimpleo annus').where('vanadyl', 'IS', 'solio tutis stultus').where('vanadyl', 'LIKE', 'cohibeo testimonium decumbo')).whereExists('ownership', q => q.where('nucleotidase', '>=', 0.7055858626438246).where('cappelletti', '>', 1284373868304998)).where('pigpen', 'sint repellendus confugo').where('wheel', 'IS', 'pecto copia blandior').where('swine', '<=', 248028722686261).where('overheard', 'IS NOT', true), {flip: true}).related('godparent', q => q.related('pantyhose', q => q.where('backbone', 'IS NOT', 'vicinus crux alias').related('pantyhose', q => q.where('vanadyl', 'charisma volup conatus').related('pantyhose', q => q.orderBy('chairperson', 'desc')).orderBy('peony', 'asc').limit(7)).orderBy('ecliptic', 'desc').orderBy('bathrobe', 'asc').limit(78)).related('ownership', q => q.where('nucleotidase', 'IS', 0.0015651051914729042).where('cappelletti', 5438505953821725).where('cappelletti', 3324134102564839).limit(150)).limit(133)).orderBy('freckle', 'desc')).limit(102)",
      ".whereExists('transom', q => q.whereExists('poetry', q => q.whereExists('orchid', q => q.where(({exists, not}) => not(exists('transom', q => q.where('valuable', '!=', 'beatae aequitas aegrotatio').where('gym', 'IS NOT', 'tondeo truculenter conatus')))).where('solvency', 'IS NOT', 'fugit cimentarius currus').where('duster', '<', 3752091419856266).where('yogurt', 2739978480801073)).where('hovel', 5972198171869894)).where('assist', '<=', 6418722340397457).where('assist', '<', 0.8066935196947392).where('gym', 'LIKE', 'officiis solvo aeternus')).where('solvency', '!=', 'viridis adicio cena').where('plain', '!=', 'ambulo doloribus dolor').orderBy('duster', 'desc').limit(161)",
      ".orderBy('disk', 'asc').orderBy('hovercraft', 'desc')",
      ".limit(25)",
    ]
  `);
});

function ast(q: AnyQueryBuilder): AST {
  return asQueryInternals(q).ast;
}
