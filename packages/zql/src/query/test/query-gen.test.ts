import {en, Faker, generateMersenne53Randomizer} from '@faker-js/faker';
import {expect, test} from 'vitest';
import type {StaticQuery} from '../static-query.ts';
import {generateQuery} from './query-gen.ts';
import {generateSchema} from './schema-gen.ts';

test('random generation', () => {
  const randomizer = generateMersenne53Randomizer(
    Date.now() ^ (Math.random() * 0x100000000),
  );
  const rng = () => randomizer.next();
  const faker = new Faker({
    locale: en,
    randomizer,
  });
  const schema = generateSchema(rng, faker);
  expect(() => generateQuery(schema, {}, rng, faker)).not.toThrow();
});

test('stable generation', () => {
  const randomizer = generateMersenne53Randomizer(42);
  const rng = () => randomizer.next();
  const faker = new Faker({
    locale: en,
    randomizer,
  });
  const schema = generateSchema(rng, faker);

  const q = generateQuery(schema, {}, rng, faker);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  expect((q as StaticQuery<any, any>).ast).toMatchInlineSnapshot(`
    {
      "limit": 129,
      "orderBy": [
        [
          "backburn",
          "asc",
        ],
      ],
      "table": "unit",
      "where": {
        "conditions": [
          {
            "op": "EXISTS",
            "related": {
              "correlation": {
                "childField": [
                  "backburn",
                ],
                "parentField": [
                  "backburn",
                ],
              },
              "subquery": {
                "alias": "zsubq_unit",
                "limit": 7,
                "orderBy": [
                  [
                    "backburn",
                    "asc",
                  ],
                ],
                "table": "unit",
                "where": {
                  "conditions": [
                    {
                      "left": {
                        "name": "substitution",
                        "type": "column",
                      },
                      "op": "LIKE",
                      "right": {
                        "type": "literal",
                        "value": "amo calcar curso",
                      },
                      "type": "simple",
                    },
                    {
                      "left": {
                        "name": "substitution",
                        "type": "column",
                      },
                      "op": "ILIKE",
                      "right": {
                        "type": "literal",
                        "value": "cur brevis animadverto",
                      },
                      "type": "simple",
                    },
                    {
                      "left": {
                        "name": "backburn",
                        "type": "column",
                      },
                      "op": "IS NOT",
                      "right": {
                        "type": "literal",
                        "value": 0.7030189588951778,
                      },
                      "type": "simple",
                    },
                    {
                      "left": {
                        "name": "backburn",
                        "type": "column",
                      },
                      "op": "IS NOT",
                      "right": {
                        "type": "literal",
                        "value": 4478816371694966,
                      },
                      "type": "simple",
                    },
                  ],
                  "type": "and",
                },
              },
              "system": "permissions",
            },
            "type": "correlatedSubquery",
          },
          {
            "op": "EXISTS",
            "related": {
              "correlation": {
                "childField": [
                  "exasperation",
                ],
                "parentField": [
                  "backburn",
                ],
              },
              "subquery": {
                "alias": "zsubq_negotiation",
                "limit": 135,
                "orderBy": [
                  [
                    "cash",
                    "asc",
                  ],
                  [
                    "brace",
                    "asc",
                  ],
                  [
                    "exasperation",
                    "asc",
                  ],
                ],
                "table": "negotiation",
                "where": {
                  "left": {
                    "name": "disk",
                    "type": "column",
                  },
                  "op": "!=",
                  "right": {
                    "type": "literal",
                    "value": "voro carbo spectaculum",
                  },
                  "type": "simple",
                },
              },
              "system": "permissions",
            },
            "type": "correlatedSubquery",
          },
        ],
        "type": "and",
      },
    }
  `);
});
