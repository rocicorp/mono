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
      "limit": 121,
      "orderBy": [
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
            "limit": 45,
            "orderBy": [
              [
                "exploration",
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
                  "left": {
                    "name": "petticoat",
                    "type": "column",
                  },
                  "op": ">",
                  "right": {
                    "type": "literal",
                    "value": 2928990975813516,
                  },
                  "type": "simple",
                },
                {
                  "left": {
                    "name": "petticoat",
                    "type": "column",
                  },
                  "op": "!=",
                  "right": {
                    "type": "literal",
                    "value": 1077209202886782,
                  },
                  "type": "simple",
                },
                {
                  "left": {
                    "name": "petticoat",
                    "type": "column",
                  },
                  "op": "IS",
                  "right": {
                    "type": "literal",
                    "value": 0.49379559636439074,
                  },
                  "type": "simple",
                },
                {
                  "left": {
                    "name": "disk",
                    "type": "column",
                  },
                  "op": "<=",
                  "right": {
                    "type": "literal",
                    "value": 283088937894669,
                  },
                  "type": "simple",
                },
              ],
              "type": "and",
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
                "limit": 77,
                "orderBy": [
                  [
                    "amendment",
                    "asc",
                  ],
                ],
                "table": "cleaner",
                "where": {
                  "conditions": [
                    {
                      "op": "EXISTS",
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
                          "limit": 73,
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
                                "limit": 47,
                                "orderBy": [
                                  [
                                    "amendment",
                                    "asc",
                                  ],
                                ],
                                "table": "cleaner",
                                "where": {
                                  "conditions": [
                                    {
                                      "op": "EXISTS",
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
                                          "limit": 60,
                                          "orderBy": [
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
                                                    "limit": 188,
                                                    "orderBy": [
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
                                                      "op": "IS",
                                                      "right": {
                                                        "type": "literal",
                                                        "value": true,
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
                                                "op": "!=",
                                                "right": {
                                                  "type": "literal",
                                                  "value": 0.9624472949421112,
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
                                      "left": {
                                        "name": "amendment",
                                        "type": "column",
                                      },
                                      "op": "IS",
                                      "right": {
                                        "type": "literal",
                                        "value": true,
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
                      "type": "correlatedSubquery",
                    },
                    {
                      "left": {
                        "name": "petticoat",
                        "type": "column",
                      },
                      "op": ">=",
                      "right": {
                        "type": "literal",
                        "value": 0.32078006497173583,
                      },
                      "type": "simple",
                    },
                    {
                      "left": {
                        "name": "amendment",
                        "type": "column",
                      },
                      "op": "=",
                      "right": {
                        "type": "literal",
                        "value": false,
                      },
                      "type": "simple",
                    },
                    {
                      "left": {
                        "name": "amendment",
                        "type": "column",
                      },
                      "op": "IS",
                      "right": {
                        "type": "literal",
                        "value": false,
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
            "left": {
              "name": "schnitzel",
              "type": "column",
            },
            "op": ">",
            "right": {
              "type": "literal",
              "value": 0.2579416277151556,
            },
            "type": "simple",
          },
          {
            "left": {
              "name": "mozzarella",
              "type": "column",
            },
            "op": "LIKE",
            "right": {
              "type": "literal",
              "value": "carbo aliquid velit",
            },
            "type": "simple",
          },
          {
            "left": {
              "name": "archaeology",
              "type": "column",
            },
            "op": "!=",
            "right": {
              "type": "literal",
              "value": 0.8971102599525771,
            },
            "type": "simple",
          },
          {
            "left": {
              "name": "archaeology",
              "type": "column",
            },
            "op": "IS",
            "right": {
              "type": "literal",
              "value": null,
            },
            "type": "simple",
          },
        ],
        "type": "and",
      },
    }
  `);
});
