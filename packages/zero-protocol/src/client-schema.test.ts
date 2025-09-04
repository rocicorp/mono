import {expect, test} from 'vitest';
import {normalizeClientSchema, type ClientSchema} from './client-schema.ts';

// Use JSON.stringify in expectations to preserve / verify key order.
const stringify = (o: unknown) => JSON.stringify(o, null, 2);

test('normalize', () => {
  const s1: ClientSchema = {
    tables: {
      b: {
        columns: {
          z: {type: 'number'},
          y: {type: 'string'},
        },
        primaryKey: ['z', 'y'],
      },
      d: {
        columns: {
          v: {type: 'null'},
          a: {type: 'null'},
          b: {type: 'json'},
        },
        primaryKey: ['b'],
      },
      g: {
        columns: {
          i: {type: 'boolean'},
          k: {type: 'string'},
          j: {type: 'json'},
        },
        primaryKey: ['k', 'i'],
      },
    },
  };

  expect(stringify(normalizeClientSchema(s1))).toMatchInlineSnapshot(`
    "{
      "tables": {
        "b": {
          "columns": {
            "y": {
              "type": "string"
            },
            "z": {
              "type": "number"
            }
          },
          "primaryKey": [
            "y",
            "z"
          ]
        },
        "d": {
          "columns": {
            "a": {
              "type": "null"
            },
            "b": {
              "type": "json"
            },
            "v": {
              "type": "null"
            }
          },
          "primaryKey": [
            "b"
          ]
        },
        "g": {
          "columns": {
            "i": {
              "type": "boolean"
            },
            "j": {
              "type": "json"
            },
            "k": {
              "type": "string"
            }
          },
          "primaryKey": [
            "i",
            "k"
          ]
        }
      }
    }"
  `);
});
