import {expect, test} from 'vitest';
import {normalize, type ClientSchema} from './client-schema.ts';

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
      },
      d: {
        columns: {
          v: {type: 'null'},
          a: {type: 'null'},
          b: {type: 'json'},
        },
      },
      g: {
        columns: {
          i: {type: 'boolean'},
          k: {type: 'string'},
          j: {type: 'json'},
        },
      },
    },
  };

  expect(stringify(normalize(s1))).toMatchInlineSnapshot(`
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
          }
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
          }
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
          }
        }
      }
    }"
  `);
});
