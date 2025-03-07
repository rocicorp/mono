import {expect, test} from 'vitest';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  string,
  table,
} from '../../zero-schema/src/builder/table-builder.ts';
import {
  clientSchemaFrom,
  normalize,
  type ClientSchema,
} from './client-schema.ts';

// Use JSON.stringify in expectations to preserve / verify key order.
const stringify = (o: unknown) => JSON.stringify(o, null, 2);

test('clientSchemaFrom', () => {
  const schema = createSchema(1, {
    tables: [
      table('issue')
        .from('issues')
        .columns({
          id: string(),
          title: string(),
          description: string(),
          closed: boolean(),
          ownerId: string().from('owner_id').optional(),
        })
        .primaryKey('id'),
      table('comment')
        .from('comments')
        .columns({
          id: string().from('comment_id'),
          issueId: string().from('the_issue_id'), // verify sorting by serverName
          description: string(),
        })
        .primaryKey('id'),
      table('noMappings')
        .columns({
          id: string(),
          description: string(),
        })
        .primaryKey('id'),
    ],
  });

  expect(stringify(clientSchemaFrom(schema))).toMatchInlineSnapshot(`
    "{
      "clientSchema": {
        "tables": {
          "comments": {
            "columns": {
              "comment_id": {
                "type": "string"
              },
              "description": {
                "type": "string"
              },
              "the_issue_id": {
                "type": "string"
              }
            }
          },
          "issues": {
            "columns": {
              "closed": {
                "type": "boolean"
              },
              "description": {
                "type": "string"
              },
              "id": {
                "type": "string"
              },
              "owner_id": {
                "type": "string"
              },
              "title": {
                "type": "string"
              }
            }
          },
          "noMappings": {
            "columns": {
              "description": {
                "type": "string"
              },
              "id": {
                "type": "string"
              }
            }
          }
        }
      },
      "hash": "qw9u2r398f0z"
    }"
  `);
});

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
