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
      "orderBy": [
        [
          "archaeology",
          "asc",
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
            "limit": 60,
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
                  "limit": 62,
                  "orderBy": [
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
                        "limit": 5,
                        "orderBy": [
                          [
                            "thorn",
                            "desc",
                          ],
                          [
                            "amendment",
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
                  "limit": 140,
                  "orderBy": [
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
                    "conditions": [
                      {
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
                            "limit": 22,
                            "orderBy": [
                              [
                                "petticoat",
                                "desc",
                              ],
                              [
                                "thorn",
                                "desc",
                              ],
                              [
                                "amendment",
                                "asc",
                              ],
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
                        "type": "correlatedSubquery",
                      },
                      {
                        "left": {
                          "name": "disk",
                          "type": "column",
                        },
                        "op": "IS",
                        "right": {
                          "type": "literal",
                          "value": 4600402723137622,
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
          },
          "system": "permissions",
        },
      ],
      "table": "negotiation",
      "where": {
        "conditions": [
          {
            "left": {
              "name": "councilman",
              "type": "column",
            },
            "op": "ILIKE",
            "right": {
              "type": "literal",
              "value": "arbor deporto voro",
            },
            "type": "simple",
          },
          {
            "left": {
              "name": "councilman",
              "type": "column",
            },
            "op": "LIKE",
            "right": {
              "type": "literal",
              "value": "capio suscipit corona",
            },
            "type": "simple",
          },
        ],
        "type": "and",
      },
    }
  `);
});
