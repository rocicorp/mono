import {expect, test} from 'vitest';
import {generateSchema} from './schema-gen.ts';
import {en, Faker, generateMersenne53Randomizer} from '@faker-js/faker';

test('stable generation', () => {
  const rng = generateMersenne53Randomizer(400);
  expect(
    generateSchema(
      () => rng.next(),
      new Faker({
        locale: en,
        randomizer: rng,
      }),
    ),
  ).toMatchInlineSnapshot(`
    {
      "relationships": {
        "adrenalin": {
          "decongestant": [
            {
              "cardinality": "many",
              "destField": [
                "community",
              ],
              "destSchema": "decongestant",
              "sourceField": [
                "dime",
              ],
            },
          ],
        },
        "chops": {},
        "decongestant": {
          "adrenalin": [
            {
              "cardinality": "many",
              "destField": [
                "dime",
              ],
              "destSchema": "adrenalin",
              "sourceField": [
                "community",
              ],
            },
          ],
        },
        "elevator": {},
        "habit": {},
        "sanity": {
          "sanity": [
            {
              "cardinality": "one",
              "destField": [
                "impostor",
              ],
              "destSchema": "sanity",
              "sourceField": [
                "impostor",
              ],
            },
          ],
        },
        "stranger": {
          "decongestant": [
            {
              "cardinality": "many",
              "destField": [
                "traffic",
              ],
              "destSchema": "decongestant",
              "sourceField": [
                "nougat",
              ],
            },
          ],
          "habit": [
            {
              "cardinality": "one",
              "destField": [
                "derby",
              ],
              "destSchema": "habit",
              "sourceField": [
                "marathon",
              ],
            },
          ],
        },
      },
      "tables": {
        "adrenalin": {
          "columns": {
            "dime": {
              "optional": true,
              "type": "string",
            },
          },
          "name": "adrenalin",
          "primaryKey": [
            "dime",
          ],
        },
        "chops": {
          "columns": {
            "gloom": {
              "optional": true,
              "type": "string",
            },
            "newsprint": {
              "optional": true,
              "type": "string",
            },
          },
          "name": "chops",
          "primaryKey": [
            "gloom",
            "newsprint",
          ],
        },
        "decongestant": {
          "columns": {
            "amnesty": {
              "optional": false,
              "type": "number",
            },
            "circumference": {
              "optional": true,
              "type": "json",
            },
            "community": {
              "optional": false,
              "type": "string",
            },
            "ghost": {
              "optional": true,
              "type": "number",
            },
            "language": {
              "optional": false,
              "type": "number",
            },
            "lyre": {
              "optional": false,
              "type": "string",
            },
            "pacemaker": {
              "optional": false,
              "type": "boolean",
            },
            "status": {
              "optional": false,
              "type": "string",
            },
            "traffic": {
              "optional": true,
              "type": "string",
            },
          },
          "name": "decongestant",
          "primaryKey": [
            "lyre",
          ],
        },
        "elevator": {
          "columns": {
            "bookcase": {
              "optional": true,
              "type": "number",
            },
            "phrase": {
              "optional": false,
              "type": "boolean",
            },
            "pliers": {
              "optional": false,
              "type": "string",
            },
            "switchboard": {
              "optional": false,
              "type": "string",
            },
            "widow": {
              "optional": true,
              "type": "number",
            },
          },
          "name": "elevator",
          "primaryKey": [
            "pliers",
            "widow",
          ],
        },
        "habit": {
          "columns": {
            "adrenalin": {
              "optional": true,
              "type": "string",
            },
            "derby": {
              "optional": false,
              "type": "number",
            },
            "independence": {
              "optional": false,
              "type": "string",
            },
            "lashes": {
              "optional": true,
              "type": "boolean",
            },
            "reporter": {
              "optional": false,
              "type": "boolean",
            },
            "resource": {
              "optional": true,
              "type": "boolean",
            },
            "sandbar": {
              "optional": true,
              "type": "string",
            },
          },
          "name": "habit",
          "primaryKey": [
            "adrenalin",
          ],
        },
        "sanity": {
          "columns": {
            "impostor": {
              "optional": false,
              "type": "number",
            },
          },
          "name": "sanity",
          "primaryKey": [
            "impostor",
          ],
        },
        "stranger": {
          "columns": {
            "footrest": {
              "optional": true,
              "type": "json",
            },
            "granny": {
              "optional": false,
              "type": "string",
            },
            "marathon": {
              "optional": false,
              "type": "number",
            },
            "nougat": {
              "optional": true,
              "type": "string",
            },
            "pile": {
              "optional": false,
              "type": "boolean",
            },
            "rawhide": {
              "optional": false,
              "type": "string",
            },
            "ruin": {
              "optional": true,
              "type": "number",
            },
            "tuber": {
              "optional": true,
              "type": "number",
            },
            "valuable": {
              "optional": false,
              "type": "number",
            },
          },
          "name": "stranger",
          "primaryKey": [
            "ruin",
          ],
        },
      },
    }
  `);
});
