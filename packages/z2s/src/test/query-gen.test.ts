import {en, Faker, generateMersenne53Randomizer} from '@faker-js/faker';
import {expect, test} from 'vitest';
import {generateSchema} from './schema-gen.ts';
import {generateQuery} from './query-gen.ts';
import type {StaticQuery} from '../../../zql/src/query/static-query.ts';

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
          "councilman",
          "desc",
        ],
        [
          "schnitzel",
          "asc",
        ],
        [
          "archaeology",
          "asc",
        ],
      ],
      "table": "negotiation",
      "where": {
        "conditions": [
          {
            "left": {
              "name": "mozzarella",
              "type": "column",
            },
            "op": "=",
            "right": {
              "type": "literal",
              "value": "urbanus qui comptus",
            },
            "type": "simple",
          },
          {
            "left": {
              "name": "schnitzel",
              "type": "column",
            },
            "op": ">",
            "right": {
              "type": "literal",
              "value": 0.8872127425763265,
            },
            "type": "simple",
          },
        ],
        "type": "and",
      },
    }
  `);
});
