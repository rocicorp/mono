import {en, Faker, generateMersenne53Randomizer} from '@faker-js/faker';
import {expect, test} from 'vitest';
import {asQueryInternals} from '../query-internals.ts';
import {generateQuery} from './query-gen.ts';
import {generateSchema} from './schema-gen.ts';

// This is flakey!!!
test.skip('random generation', () => {
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

  expect(asQueryInternals(q).ast).toMatchInlineSnapshot(`
    {
      "limit": 126,
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
                "table": "cleaner",
                "where": {
                  "conditions": [
                    {
                      "flip": undefined,
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
                          "table": "cleaner",
                          "where": {
                            "conditions": [
                              {
                                "left": {
                                  "name": "disk",
                                  "type": "column",
                                },
                                "op": "IS",
                                "right": {
                                  "type": "literal",
                                  "value": 991259612588502,
                                },
                                "type": "simple",
                              },
                              {
                                "left": {
                                  "name": "thorn",
                                  "type": "column",
                                },
                                "op": "IS",
                                "right": {
                                  "type": "literal",
                                  "value": "undique absconditus dolorem",
                                },
                                "type": "simple",
                              },
                              {
                                "left": {
                                  "name": "disk",
                                  "type": "column",
                                },
                                "op": "<",
                                "right": {
                                  "type": "literal",
                                  "value": 8492975582368892,
                                },
                                "type": "simple",
                              },
                              {
                                "left": {
                                  "name": "thorn",
                                  "type": "column",
                                },
                                "op": "IS NOT",
                                "right": {
                                  "type": "literal",
                                  "value": "convoco volup vivo",
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
                        "name": "disk",
                        "type": "column",
                      },
                      "op": ">",
                      "right": {
                        "type": "literal",
                        "value": 5490467414740416,
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
              "value": 4408598537602987,
            },
            "type": "simple",
          },
          {
            "left": {
              "name": "archaeology",
              "type": "column",
            },
            "op": "<",
            "right": {
              "type": "literal",
              "value": 6559189752506948,
            },
            "type": "simple",
          },
        ],
        "type": "and",
      },
    }
  `);
});
