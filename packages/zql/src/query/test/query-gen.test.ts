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
      "limit": 51,
      "orderBy": [
        [
          "lawmaker",
          "desc",
        ],
        [
          "schnitzel",
          "asc",
        ],
      ],
      "related": [
        {
          "correlation": {
            "childField": [
              "thorn",
            ],
            "parentField": [
              "councilman",
            ],
          },
          "subquery": {
            "alias": "cleaner",
            "limit": 85,
            "orderBy": [
              [
                "amendment",
                "asc",
              ],
            ],
            "related": [
              {
                "correlation": {
                  "childField": [
                    "amendment",
                  ],
                  "parentField": [
                    "amendment",
                  ],
                },
                "subquery": {
                  "alias": "cleaner",
                  "orderBy": [
                    [
                      "exploration",
                      "asc",
                    ],
                    [
                      "amendment",
                      "desc",
                    ],
                    [
                      "petticoat",
                      "asc",
                    ],
                  ],
                  "table": "cleaner",
                  "where": {
                    "op": "NOT EXISTS",
                    "related": {
                      "correlation": {
                        "childField": [
                          "amendment",
                        ],
                        "parentField": [
                          "amendment",
                        ],
                      },
                      "subquery": {
                        "alias": "zsubq_cleaner",
                        "orderBy": [
                          [
                            "thorn",
                            "desc",
                          ],
                          [
                            "amendment",
                            "asc",
                          ],
                          [
                            "exploration",
                            "asc",
                          ],
                        ],
                        "table": "cleaner",
                        "where": {
                          "left": {
                            "name": "thorn",
                            "type": "column",
                          },
                          "op": "LIKE",
                          "right": {
                            "type": "literal",
                            "value": "valens demens animadverto",
                          },
                          "type": "simple",
                        },
                      },
                      "system": "permissions",
                    },
                    "type": "correlatedSubquery",
                  },
                },
                "system": "permissions",
              },
            ],
            "table": "cleaner",
            "where": {
              "left": {
                "name": "amendment",
                "type": "column",
              },
              "op": "IS NOT",
              "right": {
                "type": "literal",
                "value": false,
              },
              "type": "simple",
            },
          },
          "system": "permissions",
        },
      ],
      "table": "negotiation",
      "where": {
        "conditions": [
          {
            "op": "NOT EXISTS",
            "related": {
              "correlation": {
                "childField": [
                  "thorn",
                ],
                "parentField": [
                  "councilman",
                ],
              },
              "subquery": {
                "alias": "zsubq_cleaner",
                "limit": 48,
                "orderBy": [
                  [
                    "exploration",
                    "asc",
                  ],
                  [
                    "thorn",
                    "asc",
                  ],
                  [
                    "petticoat",
                    "asc",
                  ],
                  [
                    "amendment",
                    "asc",
                  ],
                ],
                "table": "cleaner",
                "where": {
                  "op": "NOT EXISTS",
                  "related": {
                    "correlation": {
                      "childField": [
                        "amendment",
                      ],
                      "parentField": [
                        "amendment",
                      ],
                    },
                    "subquery": {
                      "alias": "zsubq_cleaner",
                      "limit": 56,
                      "orderBy": [
                        [
                          "amendment",
                          "asc",
                        ],
                        [
                          "exploration",
                          "asc",
                        ],
                      ],
                      "table": "cleaner",
                    },
                    "system": "permissions",
                  },
                  "type": "correlatedSubquery",
                },
              },
              "system": "permissions",
            },
            "type": "correlatedSubquery",
          },
          {
            "left": {
              "name": "mozzarella",
              "type": "column",
            },
            "op": "!=",
            "right": {
              "type": "literal",
              "value": "ratione recusandae facilis",
            },
            "type": "simple",
          },
          {
            "left": {
              "name": "schnitzel",
              "type": "column",
            },
            "op": "IS",
            "right": {
              "type": "literal",
              "value": 5322290477516843,
            },
            "type": "simple",
          },
          {
            "left": {
              "name": "mozzarella",
              "type": "column",
            },
            "op": "=",
            "right": {
              "type": "literal",
              "value": "similique ater studio",
            },
            "type": "simple",
          },
        ],
        "type": "and",
      },
    }
  `);
});
