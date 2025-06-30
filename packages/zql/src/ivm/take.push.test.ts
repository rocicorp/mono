import {describe, expect, suite, test} from 'vitest';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {SourceChange} from './source.ts';
import {runPushTest, type Sources} from './test/push-tests.ts';

suite('take with no partition', () => {
  suite('add', () => {
    test('limit 0', () => {
      const {data, messages, storage, pushes} = takeNoPartitionTest({
        sourceRows: [
          {id: 'i1', created: 100, text: null},
          {id: 'i2', created: 200, text: null},
          {id: 'i3', created: 300, text: null},
        ],
        limit: 0,
        pushes: [{type: 'add', row: {id: 'i4', created: 50}}],
      });
      expect(data).toMatchInlineSnapshot(`[]`);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ":source(testTable)",
            "push",
            {
              "row": {
                "created": 50,
                "id": "i4",
              },
              "type": "add",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`{}`);
      expect(pushes).toMatchInlineSnapshot(`[]`);
    });

    test('less than limit add row at start', () => {
      const {data, messages, storage, pushes} = takeNoPartitionTest({
        sourceRows: [
          {id: 'i1', created: 100, text: null},
          {id: 'i2', created: 200, text: null},
          {id: 'i3', created: 300, text: null},
        ],
        limit: 5,
        pushes: [{type: 'add', row: {id: 'i4', created: 50}}],
      });
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "created": 50,
            "id": "i4",
            Symbol(rc): 1,
          },
          {
            "created": 100,
            "id": "i1",
            "text": null,
            Symbol(rc): 1,
          },
          {
            "created": 200,
            "id": "i2",
            "text": null,
            Symbol(rc): 1,
          },
          {
            "created": 300,
            "id": "i3",
            "text": null,
            Symbol(rc): 1,
          },
        ]
      `);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ":source(testTable)",
            "push",
            {
              "row": {
                "created": 50,
                "id": "i4",
              },
              "type": "add",
            },
          ],
          [
            ":take",
            "push",
            {
              "row": {
                "created": 50,
                "id": "i4",
              },
              "type": "add",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        {
          "["take"]": {
            "bound": {
              "created": 300,
              "id": "i3",
              "text": null,
            },
            "size": 4,
          },
          "maxBound": {
            "created": 300,
            "id": "i3",
            "text": null,
          },
        }
      `);
      expect(pushes).toMatchInlineSnapshot(`
        [
          {
            "node": {
              "relationships": {},
              "row": {
                "created": 50,
                "id": "i4",
              },
            },
            "type": "add",
          },
        ]
      `);
    });

    test('less than limit add row at end', () => {
      const {data, messages, storage, pushes} = takeNoPartitionTest({
        sourceRows: [
          {id: 'i1', created: 100, text: null},
          {id: 'i2', created: 200, text: null},
          {id: 'i3', created: 300, text: null},
        ],
        limit: 5,
        pushes: [{type: 'add', row: {id: 'i4', created: 350}}],
      });
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "created": 100,
            "id": "i1",
            "text": null,
            Symbol(rc): 1,
          },
          {
            "created": 200,
            "id": "i2",
            "text": null,
            Symbol(rc): 1,
          },
          {
            "created": 300,
            "id": "i3",
            "text": null,
            Symbol(rc): 1,
          },
          {
            "created": 350,
            "id": "i4",
            Symbol(rc): 1,
          },
        ]
      `);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ":source(testTable)",
            "push",
            {
              "row": {
                "created": 350,
                "id": "i4",
              },
              "type": "add",
            },
          ],
          [
            ":take",
            "push",
            {
              "row": {
                "created": 350,
                "id": "i4",
              },
              "type": "add",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        {
          "["take"]": {
            "bound": {
              "created": 350,
              "id": "i4",
            },
            "size": 4,
          },
          "maxBound": {
            "created": 350,
            "id": "i4",
          },
        }
      `);
      expect(pushes).toMatchInlineSnapshot(`
        [
          {
            "node": {
              "relationships": {},
              "row": {
                "created": 350,
                "id": "i4",
              },
            },
            "type": "add",
          },
        ]
      `);
    });

    test('at limit add row after bound', () => {
      const {data, messages, storage, pushes} = takeNoPartitionTest({
        sourceRows: [
          {id: 'i1', created: 100, text: null},
          {id: 'i2', created: 200, text: null},
          {id: 'i3', created: 300, text: null},
          {id: 'i4', created: 400, text: null},
        ],
        limit: 3,
        pushes: [{type: 'add', row: {id: 'i5', created: 350}}],
      });
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "created": 100,
            "id": "i1",
            "text": null,
            Symbol(rc): 1,
          },
          {
            "created": 200,
            "id": "i2",
            "text": null,
            Symbol(rc): 1,
          },
          {
            "created": 300,
            "id": "i3",
            "text": null,
            Symbol(rc): 1,
          },
        ]
      `);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ":source(testTable)",
            "push",
            {
              "row": {
                "created": 350,
                "id": "i5",
              },
              "type": "add",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        {
          "["take"]": {
            "bound": {
              "created": 300,
              "id": "i3",
              "text": null,
            },
            "size": 3,
          },
          "maxBound": {
            "created": 300,
            "id": "i3",
            "text": null,
          },
        }
      `);
      expect(pushes).toMatchInlineSnapshot(`[]`);
    });

    test('at limit add row at start', () => {
      const {data, messages, storage, pushesWithFetch} = takeNoPartitionTest({
        sourceRows: [
          {id: 'i1', created: 100, text: null},
          {id: 'i2', created: 200, text: null},
          {id: 'i3', created: 300, text: null},
          {id: 'i4', created: 400, text: null},
        ],
        limit: 3,
        pushes: [{type: 'add', row: {id: 'i5', created: 50}}],
        fetchOnPush: true,
      });
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "created": 50,
            "id": "i5",
            Symbol(rc): 1,
          },
          {
            "created": 100,
            "id": "i1",
            "text": null,
            Symbol(rc): 1,
          },
          {
            "created": 200,
            "id": "i2",
            "text": null,
            Symbol(rc): 1,
          },
        ]
      `);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ":source(testTable)",
            "push",
            {
              "row": {
                "created": 50,
                "id": "i5",
              },
              "type": "add",
            },
          ],
          [
            ":source(testTable)",
            "fetch",
            {
              "constraint": undefined,
              "reverse": true,
              "start": {
                "basis": "at",
                "row": {
                  "created": 300,
                  "id": "i3",
                  "text": null,
                },
              },
            },
          ],
          [
            ":take",
            "push",
            {
              "row": {
                "created": 300,
                "id": "i3",
                "text": null,
              },
              "type": "remove",
            },
          ],
          [
            ":take",
            "push",
            {
              "row": {
                "created": 50,
                "id": "i5",
              },
              "type": "add",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        {
          "["take"]": {
            "bound": {
              "created": 200,
              "id": "i2",
              "text": null,
            },
            "size": 3,
          },
          "maxBound": {
            "created": 300,
            "id": "i3",
            "text": null,
          },
        }
      `);
      expect(pushesWithFetch).toMatchInlineSnapshot(`
        [
          {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "created": 300,
                  "id": "i3",
                  "text": null,
                },
              },
              "type": "remove",
            },
            "fetch": [
              {
                "relationships": {},
                "row": {
                  "created": 100,
                  "id": "i1",
                  "text": null,
                },
              },
              {
                "relationships": {},
                "row": {
                  "created": 200,
                  "id": "i2",
                  "text": null,
                },
              },
            ],
          },
          {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "created": 50,
                  "id": "i5",
                },
              },
              "type": "add",
            },
            "fetch": [
              {
                "relationships": {},
                "row": {
                  "created": 50,
                  "id": "i5",
                },
              },
              {
                "relationships": {},
                "row": {
                  "created": 100,
                  "id": "i1",
                  "text": null,
                },
              },
              {
                "relationships": {},
                "row": {
                  "created": 200,
                  "id": "i2",
                  "text": null,
                },
              },
            ],
          },
        ]
      `);
    });

    test('at limit add row at start, limit 1', () => {
      const {data, messages, storage, pushesWithFetch} = takeNoPartitionTest({
        sourceRows: [
          {id: 'i1', created: 100, text: null},
          {id: 'i2', created: 200, text: null},
          {id: 'i3', created: 300, text: null},
          {id: 'i4', created: 400, text: null},
        ],
        limit: 1,
        pushes: [{type: 'add', row: {id: 'i5', created: 50}}],
        fetchOnPush: true,
      });
      expect(data).toMatchInlineSnapshot(`
        {
          "created": 50,
          "id": "i5",
          Symbol(rc): 1,
        }
      `);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ":source(testTable)",
            "push",
            {
              "row": {
                "created": 50,
                "id": "i5",
              },
              "type": "add",
            },
          ],
          [
            ":source(testTable)",
            "fetch",
            {
              "constraint": undefined,
              "start": {
                "basis": "at",
                "row": {
                  "created": 100,
                  "id": "i1",
                  "text": null,
                },
              },
            },
          ],
          [
            ":take",
            "push",
            {
              "row": {
                "created": 100,
                "id": "i1",
                "text": null,
              },
              "type": "remove",
            },
          ],
          [
            ":take",
            "push",
            {
              "row": {
                "created": 50,
                "id": "i5",
              },
              "type": "add",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        {
          "["take"]": {
            "bound": {
              "created": 50,
              "id": "i5",
            },
            "size": 1,
          },
          "maxBound": {
            "created": 100,
            "id": "i1",
            "text": null,
          },
        }
      `);
      expect(pushesWithFetch).toMatchInlineSnapshot(`
        [
          {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "created": 100,
                  "id": "i1",
                  "text": null,
                },
              },
              "type": "remove",
            },
            "fetch": [],
          },
          {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "created": 50,
                  "id": "i5",
                },
              },
              "type": "add",
            },
            "fetch": [
              {
                "relationships": {},
                "row": {
                  "created": 50,
                  "id": "i5",
                },
              },
            ],
          },
        ]
      `);
    });

    test('at limit add row at end', () => {
      const {data, messages, storage, pushesWithFetch} = takeNoPartitionTest({
        sourceRows: [
          {id: 'i1', created: 100, text: null},
          {id: 'i2', created: 200, text: null},
          {id: 'i3', created: 300, text: null},
          {id: 'i4', created: 400, text: null},
        ],
        limit: 3,
        pushes: [{type: 'add', row: {id: 'i5', created: 250}}],
        fetchOnPush: true,
      });
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "created": 100,
            "id": "i1",
            "text": null,
            Symbol(rc): 1,
          },
          {
            "created": 200,
            "id": "i2",
            "text": null,
            Symbol(rc): 1,
          },
          {
            "created": 250,
            "id": "i5",
            Symbol(rc): 1,
          },
        ]
      `);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ":source(testTable)",
            "push",
            {
              "row": {
                "created": 250,
                "id": "i5",
              },
              "type": "add",
            },
          ],
          [
            ":source(testTable)",
            "fetch",
            {
              "constraint": undefined,
              "reverse": true,
              "start": {
                "basis": "at",
                "row": {
                  "created": 300,
                  "id": "i3",
                  "text": null,
                },
              },
            },
          ],
          [
            ":take",
            "push",
            {
              "row": {
                "created": 300,
                "id": "i3",
                "text": null,
              },
              "type": "remove",
            },
          ],
          [
            ":take",
            "push",
            {
              "row": {
                "created": 250,
                "id": "i5",
              },
              "type": "add",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        {
          "["take"]": {
            "bound": {
              "created": 250,
              "id": "i5",
            },
            "size": 3,
          },
          "maxBound": {
            "created": 300,
            "id": "i3",
            "text": null,
          },
        }
      `);
      expect(pushesWithFetch).toMatchInlineSnapshot(`
        [
          {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "created": 300,
                  "id": "i3",
                  "text": null,
                },
              },
              "type": "remove",
            },
            "fetch": [
              {
                "relationships": {},
                "row": {
                  "created": 100,
                  "id": "i1",
                  "text": null,
                },
              },
              {
                "relationships": {},
                "row": {
                  "created": 200,
                  "id": "i2",
                  "text": null,
                },
              },
            ],
          },
          {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "created": 250,
                  "id": "i5",
                },
              },
              "type": "add",
            },
            "fetch": [
              {
                "relationships": {},
                "row": {
                  "created": 100,
                  "id": "i1",
                  "text": null,
                },
              },
              {
                "relationships": {},
                "row": {
                  "created": 200,
                  "id": "i2",
                  "text": null,
                },
              },
              {
                "relationships": {},
                "row": {
                  "created": 250,
                  "id": "i5",
                },
              },
            ],
          },
        ]
      `);
    });
  });

  suite('remove', () => {
    test('limit 0', () => {
      const {data, messages, storage, pushes} = takeNoPartitionTest({
        sourceRows: [
          {id: 'i1', created: 100, text: null},
          {id: 'i2', created: 200, text: null},
          {id: 'i3', created: 300, text: null},
        ],
        limit: 0,
        pushes: [{type: 'remove', row: {id: 'i1', created: 100}}],
      });
      expect(data).toMatchInlineSnapshot(`[]`);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ":source(testTable)",
            "push",
            {
              "row": {
                "created": 100,
                "id": "i1",
              },
              "type": "remove",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`{}`);
      expect(pushes).toMatchInlineSnapshot(`[]`);
    });

    test('less than limit remove row at start', () => {
      const {data, messages, storage, pushes} = takeNoPartitionTest({
        sourceRows: [
          {id: 'i1', created: 100, text: null},
          {id: 'i2', created: 200, text: null},
          {id: 'i3', created: 300, text: null},
        ],
        limit: 5,
        pushes: [{type: 'remove', row: {id: 'i1', created: 100}}],
      });
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "created": 200,
            "id": "i2",
            "text": null,
            Symbol(rc): 1,
          },
          {
            "created": 300,
            "id": "i3",
            "text": null,
            Symbol(rc): 1,
          },
        ]
      `);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ":source(testTable)",
            "push",
            {
              "row": {
                "created": 100,
                "id": "i1",
              },
              "type": "remove",
            },
          ],
          [
            ":source(testTable)",
            "fetch",
            {
              "constraint": undefined,
              "reverse": true,
              "start": {
                "basis": "after",
                "row": {
                  "created": 300,
                  "id": "i3",
                  "text": null,
                },
              },
            },
          ],
          [
            ":source(testTable)",
            "fetch",
            {
              "constraint": undefined,
              "start": {
                "basis": "at",
                "row": {
                  "created": 300,
                  "id": "i3",
                  "text": null,
                },
              },
            },
          ],
          [
            ":take",
            "push",
            {
              "row": {
                "created": 100,
                "id": "i1",
              },
              "type": "remove",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        {
          "["take"]": {
            "bound": {
              "created": 300,
              "id": "i3",
              "text": null,
            },
            "size": 2,
          },
          "maxBound": {
            "created": 300,
            "id": "i3",
            "text": null,
          },
        }
      `);
      expect(pushes).toMatchInlineSnapshot(`
        [
          {
            "node": {
              "relationships": {},
              "row": {
                "created": 100,
                "id": "i1",
              },
            },
            "type": "remove",
          },
        ]
      `);
    });

    test('less than limit remove row at end', () => {
      const {data, messages, storage, pushes} = takeNoPartitionTest({
        sourceRows: [
          {id: 'i1', created: 100, text: null},
          {id: 'i2', created: 200, text: null},
          {id: 'i3', created: 300, text: null},
        ],
        limit: 5,
        pushes: [{type: 'remove', row: {id: 'i3', created: 300}}],
      });
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "created": 100,
            "id": "i1",
            "text": null,
            Symbol(rc): 1,
          },
          {
            "created": 200,
            "id": "i2",
            "text": null,
            Symbol(rc): 1,
          },
        ]
      `);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ":source(testTable)",
            "push",
            {
              "row": {
                "created": 300,
                "id": "i3",
              },
              "type": "remove",
            },
          ],
          [
            ":source(testTable)",
            "fetch",
            {
              "constraint": undefined,
              "reverse": true,
              "start": {
                "basis": "after",
                "row": {
                  "created": 300,
                  "id": "i3",
                  "text": null,
                },
              },
            },
          ],
          [
            ":source(testTable)",
            "fetch",
            {
              "constraint": undefined,
              "start": {
                "basis": "at",
                "row": {
                  "created": 300,
                  "id": "i3",
                  "text": null,
                },
              },
            },
          ],
          [
            ":take",
            "push",
            {
              "row": {
                "created": 300,
                "id": "i3",
              },
              "type": "remove",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        {
          "["take"]": {
            "bound": {
              "created": 200,
              "id": "i2",
              "text": null,
            },
            "size": 2,
          },
          "maxBound": {
            "created": 300,
            "id": "i3",
            "text": null,
          },
        }
      `);
      expect(pushes).toMatchInlineSnapshot(`
        [
          {
            "node": {
              "relationships": {},
              "row": {
                "created": 300,
                "id": "i3",
              },
            },
            "type": "remove",
          },
        ]
      `);
    });

    test('at limit remove row after bound', () => {
      const {data, messages, storage, pushes} = takeNoPartitionTest({
        sourceRows: [
          {id: 'i1', created: 100, text: null},
          {id: 'i2', created: 200, text: null},
          {id: 'i3', created: 300, text: null},
          {id: 'i4', created: 400, text: null},
        ],
        limit: 3,
        pushes: [{type: 'remove', row: {id: 'i4', created: 400}}],
      });
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "created": 100,
            "id": "i1",
            "text": null,
            Symbol(rc): 1,
          },
          {
            "created": 200,
            "id": "i2",
            "text": null,
            Symbol(rc): 1,
          },
          {
            "created": 300,
            "id": "i3",
            "text": null,
            Symbol(rc): 1,
          },
        ]
      `);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ":source(testTable)",
            "push",
            {
              "row": {
                "created": 400,
                "id": "i4",
              },
              "type": "remove",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        {
          "["take"]": {
            "bound": {
              "created": 300,
              "id": "i3",
              "text": null,
            },
            "size": 3,
          },
          "maxBound": {
            "created": 300,
            "id": "i3",
            "text": null,
          },
        }
      `);
      expect(pushes).toMatchInlineSnapshot(`[]`);
    });

    test('at limit remove row at start with row after', () => {
      const {data, messages, storage, pushesWithFetch} = takeNoPartitionTest({
        sourceRows: [
          {id: 'i1', created: 100, text: null},
          {id: 'i2', created: 200, text: null},
          {id: 'i3', created: 300, text: null},
          {id: 'i4', created: 400, text: null},
        ],
        limit: 3,
        pushes: [{type: 'remove', row: {id: 'i1', created: 100}}],
        fetchOnPush: true,
      });
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "created": 200,
            "id": "i2",
            "text": null,
            Symbol(rc): 1,
          },
          {
            "created": 300,
            "id": "i3",
            "text": null,
            Symbol(rc): 1,
          },
          {
            "created": 400,
            "id": "i4",
            "text": null,
            Symbol(rc): 1,
          },
        ]
      `);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ":source(testTable)",
            "push",
            {
              "row": {
                "created": 100,
                "id": "i1",
              },
              "type": "remove",
            },
          ],
          [
            ":source(testTable)",
            "fetch",
            {
              "constraint": undefined,
              "reverse": true,
              "start": {
                "basis": "after",
                "row": {
                  "created": 300,
                  "id": "i3",
                  "text": null,
                },
              },
            },
          ],
          [
            ":source(testTable)",
            "fetch",
            {
              "constraint": undefined,
              "start": {
                "basis": "at",
                "row": {
                  "created": 300,
                  "id": "i3",
                  "text": null,
                },
              },
            },
          ],
          [
            ":take",
            "push",
            {
              "row": {
                "created": 100,
                "id": "i1",
              },
              "type": "remove",
            },
          ],
          [
            ":take",
            "push",
            {
              "row": {
                "created": 400,
                "id": "i4",
                "text": null,
              },
              "type": "add",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        {
          "["take"]": {
            "bound": {
              "created": 400,
              "id": "i4",
              "text": null,
            },
            "size": 3,
          },
          "maxBound": {
            "created": 400,
            "id": "i4",
            "text": null,
          },
        }
      `);
      expect(pushesWithFetch).toMatchInlineSnapshot(`
        [
          {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "created": 100,
                  "id": "i1",
                },
              },
              "type": "remove",
            },
            "fetch": [
              {
                "relationships": {},
                "row": {
                  "created": 200,
                  "id": "i2",
                  "text": null,
                },
              },
              {
                "relationships": {},
                "row": {
                  "created": 300,
                  "id": "i3",
                  "text": null,
                },
              },
            ],
          },
          {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "created": 400,
                  "id": "i4",
                  "text": null,
                },
              },
              "type": "add",
            },
            "fetch": [
              {
                "relationships": {},
                "row": {
                  "created": 200,
                  "id": "i2",
                  "text": null,
                },
              },
              {
                "relationships": {},
                "row": {
                  "created": 300,
                  "id": "i3",
                  "text": null,
                },
              },
              {
                "relationships": {},
                "row": {
                  "created": 400,
                  "id": "i4",
                  "text": null,
                },
              },
            ],
          },
        ]
      `);
    });

    test('at limit remove row at start with row after, limit 2', () => {
      const {data, messages, storage, pushesWithFetch} = takeNoPartitionTest({
        sourceRows: [
          {id: 'i1', created: 100, text: null},
          {id: 'i2', created: 200, text: null},
          {id: 'i3', created: 300, text: null},
          {id: 'i4', created: 400, text: null},
        ],
        limit: 2,
        pushes: [{type: 'remove', row: {id: 'i1', created: 100}}],
        fetchOnPush: true,
      });
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "created": 200,
            "id": "i2",
            "text": null,
            Symbol(rc): 1,
          },
          {
            "created": 300,
            "id": "i3",
            "text": null,
            Symbol(rc): 1,
          },
        ]
      `);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ":source(testTable)",
            "push",
            {
              "row": {
                "created": 100,
                "id": "i1",
              },
              "type": "remove",
            },
          ],
          [
            ":source(testTable)",
            "fetch",
            {
              "constraint": undefined,
              "reverse": true,
              "start": {
                "basis": "after",
                "row": {
                  "created": 200,
                  "id": "i2",
                  "text": null,
                },
              },
            },
          ],
          [
            ":source(testTable)",
            "fetch",
            {
              "constraint": undefined,
              "start": {
                "basis": "at",
                "row": {
                  "created": 200,
                  "id": "i2",
                  "text": null,
                },
              },
            },
          ],
          [
            ":take",
            "push",
            {
              "row": {
                "created": 100,
                "id": "i1",
              },
              "type": "remove",
            },
          ],
          [
            ":take",
            "push",
            {
              "row": {
                "created": 300,
                "id": "i3",
                "text": null,
              },
              "type": "add",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        {
          "["take"]": {
            "bound": {
              "created": 300,
              "id": "i3",
              "text": null,
            },
            "size": 2,
          },
          "maxBound": {
            "created": 300,
            "id": "i3",
            "text": null,
          },
        }
      `);
      expect(pushesWithFetch).toMatchInlineSnapshot(`
        [
          {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "created": 100,
                  "id": "i1",
                },
              },
              "type": "remove",
            },
            "fetch": [
              {
                "relationships": {},
                "row": {
                  "created": 200,
                  "id": "i2",
                  "text": null,
                },
              },
            ],
          },
          {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "created": 300,
                  "id": "i3",
                  "text": null,
                },
              },
              "type": "add",
            },
            "fetch": [
              {
                "relationships": {},
                "row": {
                  "created": 200,
                  "id": "i2",
                  "text": null,
                },
              },
              {
                "relationships": {},
                "row": {
                  "created": 300,
                  "id": "i3",
                  "text": null,
                },
              },
            ],
          },
        ]
      `);
    });

    test('at limit remove row at start with row after, limit 1', () => {
      const {data, messages, storage, pushesWithFetch} = takeNoPartitionTest({
        sourceRows: [
          {id: 'i1', created: 100, text: null},
          {id: 'i2', created: 200, text: null},
          {id: 'i3', created: 300, text: null},
          {id: 'i4', created: 400, text: null},
        ],
        limit: 1,
        pushes: [{type: 'remove', row: {id: 'i1', created: 100}}],
        fetchOnPush: true,
      });
      expect(data).toMatchInlineSnapshot(`
        {
          "created": 200,
          "id": "i2",
          "text": null,
          Symbol(rc): 1,
        }
      `);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ":source(testTable)",
            "push",
            {
              "row": {
                "created": 100,
                "id": "i1",
              },
              "type": "remove",
            },
          ],
          [
            ":source(testTable)",
            "fetch",
            {
              "constraint": undefined,
              "reverse": true,
              "start": {
                "basis": "after",
                "row": {
                  "created": 100,
                  "id": "i1",
                  "text": null,
                },
              },
            },
          ],
          [
            ":source(testTable)",
            "fetch",
            {
              "constraint": undefined,
              "start": {
                "basis": "at",
                "row": {
                  "created": 100,
                  "id": "i1",
                  "text": null,
                },
              },
            },
          ],
          [
            ":take",
            "push",
            {
              "row": {
                "created": 100,
                "id": "i1",
              },
              "type": "remove",
            },
          ],
          [
            ":take",
            "push",
            {
              "row": {
                "created": 200,
                "id": "i2",
                "text": null,
              },
              "type": "add",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        {
          "["take"]": {
            "bound": {
              "created": 200,
              "id": "i2",
              "text": null,
            },
            "size": 1,
          },
          "maxBound": {
            "created": 200,
            "id": "i2",
            "text": null,
          },
        }
      `);
      expect(pushesWithFetch).toMatchInlineSnapshot(`
        [
          {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "created": 100,
                  "id": "i1",
                },
              },
              "type": "remove",
            },
            "fetch": [],
          },
          {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "created": 200,
                  "id": "i2",
                  "text": null,
                },
              },
              "type": "add",
            },
            "fetch": [
              {
                "relationships": {},
                "row": {
                  "created": 200,
                  "id": "i2",
                  "text": null,
                },
              },
            ],
          },
        ]
      `);
    });

    test('at limit remove row at start no row after', () => {
      const {data, messages, storage, pushes} = takeNoPartitionTest({
        sourceRows: [
          {id: 'i1', created: 100, text: null},
          {id: 'i2', created: 200, text: null},
          {id: 'i3', created: 300, text: null},
        ],
        limit: 3,
        pushes: [{type: 'remove', row: {id: 'i1', created: 100}}],
      });
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "created": 200,
            "id": "i2",
            "text": null,
            Symbol(rc): 1,
          },
          {
            "created": 300,
            "id": "i3",
            "text": null,
            Symbol(rc): 1,
          },
        ]
      `);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ":source(testTable)",
            "push",
            {
              "row": {
                "created": 100,
                "id": "i1",
              },
              "type": "remove",
            },
          ],
          [
            ":source(testTable)",
            "fetch",
            {
              "constraint": undefined,
              "reverse": true,
              "start": {
                "basis": "after",
                "row": {
                  "created": 300,
                  "id": "i3",
                  "text": null,
                },
              },
            },
          ],
          [
            ":source(testTable)",
            "fetch",
            {
              "constraint": undefined,
              "start": {
                "basis": "at",
                "row": {
                  "created": 300,
                  "id": "i3",
                  "text": null,
                },
              },
            },
          ],
          [
            ":take",
            "push",
            {
              "row": {
                "created": 100,
                "id": "i1",
              },
              "type": "remove",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        {
          "["take"]": {
            "bound": {
              "created": 300,
              "id": "i3",
              "text": null,
            },
            "size": 2,
          },
          "maxBound": {
            "created": 300,
            "id": "i3",
            "text": null,
          },
        }
      `);
      expect(pushes).toMatchInlineSnapshot(`
        [
          {
            "node": {
              "relationships": {},
              "row": {
                "created": 100,
                "id": "i1",
              },
            },
            "type": "remove",
          },
        ]
      `);
    });

    test('at limit remove row at end with row after', () => {
      const {data, messages, storage, pushesWithFetch} = takeNoPartitionTest({
        sourceRows: [
          {id: 'i1', created: 100, text: null},
          {id: 'i2', created: 200, text: null},
          {id: 'i3', created: 300, text: null},
          {id: 'i4', created: 400, text: null},
        ],
        limit: 3,
        pushes: [{type: 'remove', row: {id: 'i3', created: 300}}],
        fetchOnPush: true,
      });
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "created": 100,
            "id": "i1",
            "text": null,
            Symbol(rc): 1,
          },
          {
            "created": 200,
            "id": "i2",
            "text": null,
            Symbol(rc): 1,
          },
          {
            "created": 400,
            "id": "i4",
            "text": null,
            Symbol(rc): 1,
          },
        ]
      `);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ":source(testTable)",
            "push",
            {
              "row": {
                "created": 300,
                "id": "i3",
              },
              "type": "remove",
            },
          ],
          [
            ":source(testTable)",
            "fetch",
            {
              "constraint": undefined,
              "reverse": true,
              "start": {
                "basis": "after",
                "row": {
                  "created": 300,
                  "id": "i3",
                  "text": null,
                },
              },
            },
          ],
          [
            ":source(testTable)",
            "fetch",
            {
              "constraint": undefined,
              "start": {
                "basis": "at",
                "row": {
                  "created": 300,
                  "id": "i3",
                  "text": null,
                },
              },
            },
          ],
          [
            ":take",
            "push",
            {
              "row": {
                "created": 300,
                "id": "i3",
              },
              "type": "remove",
            },
          ],
          [
            ":take",
            "push",
            {
              "row": {
                "created": 400,
                "id": "i4",
                "text": null,
              },
              "type": "add",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        {
          "["take"]": {
            "bound": {
              "created": 400,
              "id": "i4",
              "text": null,
            },
            "size": 3,
          },
          "maxBound": {
            "created": 400,
            "id": "i4",
            "text": null,
          },
        }
      `);
      expect(pushesWithFetch).toMatchInlineSnapshot(`
        [
          {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "created": 300,
                  "id": "i3",
                },
              },
              "type": "remove",
            },
            "fetch": [
              {
                "relationships": {},
                "row": {
                  "created": 100,
                  "id": "i1",
                  "text": null,
                },
              },
              {
                "relationships": {},
                "row": {
                  "created": 200,
                  "id": "i2",
                  "text": null,
                },
              },
            ],
          },
          {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "created": 400,
                  "id": "i4",
                  "text": null,
                },
              },
              "type": "add",
            },
            "fetch": [
              {
                "relationships": {},
                "row": {
                  "created": 100,
                  "id": "i1",
                  "text": null,
                },
              },
              {
                "relationships": {},
                "row": {
                  "created": 200,
                  "id": "i2",
                  "text": null,
                },
              },
              {
                "relationships": {},
                "row": {
                  "created": 400,
                  "id": "i4",
                  "text": null,
                },
              },
            ],
          },
        ]
      `);
    });

    test('at limit remove row at end, no row after', () => {
      const {data, messages, storage, pushes} = takeNoPartitionTest({
        sourceRows: [
          {id: 'i1', created: 100, text: null},
          {id: 'i2', created: 200, text: null},
          {id: 'i3', created: 300, text: null},
        ],
        limit: 3,
        pushes: [{type: 'remove', row: {id: 'i3', created: 300}}],
      });
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "created": 100,
            "id": "i1",
            "text": null,
            Symbol(rc): 1,
          },
          {
            "created": 200,
            "id": "i2",
            "text": null,
            Symbol(rc): 1,
          },
        ]
      `);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ":source(testTable)",
            "push",
            {
              "row": {
                "created": 300,
                "id": "i3",
              },
              "type": "remove",
            },
          ],
          [
            ":source(testTable)",
            "fetch",
            {
              "constraint": undefined,
              "reverse": true,
              "start": {
                "basis": "after",
                "row": {
                  "created": 300,
                  "id": "i3",
                  "text": null,
                },
              },
            },
          ],
          [
            ":source(testTable)",
            "fetch",
            {
              "constraint": undefined,
              "start": {
                "basis": "at",
                "row": {
                  "created": 300,
                  "id": "i3",
                  "text": null,
                },
              },
            },
          ],
          [
            ":take",
            "push",
            {
              "row": {
                "created": 300,
                "id": "i3",
              },
              "type": "remove",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        {
          "["take"]": {
            "bound": {
              "created": 200,
              "id": "i2",
              "text": null,
            },
            "size": 2,
          },
          "maxBound": {
            "created": 300,
            "id": "i3",
            "text": null,
          },
        }
      `);
      expect(pushes).toMatchInlineSnapshot(`
        [
          {
            "node": {
              "relationships": {},
              "row": {
                "created": 300,
                "id": "i3",
              },
            },
            "type": "remove",
          },
        ]
      `);
    });
  });

  suite('edit', () => {
    const base = {
      sourceRows: [
        {id: 'i1', created: 100, text: 'a'},
        {id: 'i2', created: 200, text: 'b'},
        {id: 'i3', created: 300, text: 'c'},
        {id: 'i4', created: 400, text: 'd'},
      ],
    } as const;

    test('limit 0', () => {
      const {data, messages, storage, pushes} = takeNoPartitionTest({
        ...base,
        limit: 0,
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'i2', created: 200, text: 'b'},
            row: {id: 'i2', created: 200, text: 'c'},
          },
        ],
      });
      expect(data).toMatchInlineSnapshot(`[]`);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ":source(testTable)",
            "push",
            {
              "oldRow": {
                "created": 200,
                "id": "i2",
                "text": "b",
              },
              "row": {
                "created": 200,
                "id": "i2",
                "text": "c",
              },
              "type": "edit",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`{}`);
      expect(pushes).toMatchInlineSnapshot(`[]`);
    });

    describe('less than limit ', () => {
      test('edit row at start', () => {
        const {data, messages, storage, pushes} = takeNoPartitionTest({
          ...base,
          limit: 5,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'i1', created: 100, text: 'a'},
              row: {id: 'i1', created: 100, text: 'a2'},
            },
          ],
        });
        expect(data).toMatchInlineSnapshot(`
          [
            {
              "created": 100,
              "id": "i1",
              "text": "a2",
              Symbol(rc): 1,
            },
            {
              "created": 200,
              "id": "i2",
              "text": "b",
              Symbol(rc): 1,
            },
            {
              "created": 300,
              "id": "i3",
              "text": "c",
              Symbol(rc): 1,
            },
            {
              "created": 400,
              "id": "i4",
              "text": "d",
              Symbol(rc): 1,
            },
          ]
        `);
        expect(messages).toMatchInlineSnapshot(`
          [
            [
              ":source(testTable)",
              "push",
              {
                "oldRow": {
                  "created": 100,
                  "id": "i1",
                  "text": "a",
                },
                "row": {
                  "created": 100,
                  "id": "i1",
                  "text": "a2",
                },
                "type": "edit",
              },
            ],
            [
              ":take",
              "push",
              {
                "oldRow": {
                  "created": 100,
                  "id": "i1",
                  "text": "a",
                },
                "row": {
                  "created": 100,
                  "id": "i1",
                  "text": "a2",
                },
                "type": "edit",
              },
            ],
          ]
        `);
        expect(storage).toMatchInlineSnapshot(`
          {
            "["take"]": {
              "bound": {
                "created": 400,
                "id": "i4",
                "text": "d",
              },
              "size": 4,
            },
            "maxBound": {
              "created": 400,
              "id": "i4",
              "text": "d",
            },
          }
        `);
        expect(pushes).toMatchInlineSnapshot(`
          [
            {
              "oldRow": {
                "created": 100,
                "id": "i1",
                "text": "a",
              },
              "row": {
                "created": 100,
                "id": "i1",
                "text": "a2",
              },
              "type": "edit",
            },
          ]
        `);
      });

      test('edit row at end', () => {
        const {data, messages, storage, pushes} = takeNoPartitionTest({
          ...base,
          limit: 5,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'i4', created: 400, text: 'd'},
              row: {id: 'i4', created: 400, text: 'd2'},
            },
          ],
        });
        expect(data).toMatchInlineSnapshot(`
          [
            {
              "created": 100,
              "id": "i1",
              "text": "a",
              Symbol(rc): 1,
            },
            {
              "created": 200,
              "id": "i2",
              "text": "b",
              Symbol(rc): 1,
            },
            {
              "created": 300,
              "id": "i3",
              "text": "c",
              Symbol(rc): 1,
            },
            {
              "created": 400,
              "id": "i4",
              "text": "d2",
              Symbol(rc): 1,
            },
          ]
        `);
        expect(messages).toMatchInlineSnapshot(`
          [
            [
              ":source(testTable)",
              "push",
              {
                "oldRow": {
                  "created": 400,
                  "id": "i4",
                  "text": "d",
                },
                "row": {
                  "created": 400,
                  "id": "i4",
                  "text": "d2",
                },
                "type": "edit",
              },
            ],
            [
              ":take",
              "push",
              {
                "oldRow": {
                  "created": 400,
                  "id": "i4",
                  "text": "d",
                },
                "row": {
                  "created": 400,
                  "id": "i4",
                  "text": "d2",
                },
                "type": "edit",
              },
            ],
          ]
        `);
        expect(storage).toMatchInlineSnapshot(`
          {
            "["take"]": {
              "bound": {
                "created": 400,
                "id": "i4",
                "text": "d",
              },
              "size": 4,
            },
            "maxBound": {
              "created": 400,
              "id": "i4",
              "text": "d",
            },
          }
        `);
        expect(pushes).toMatchInlineSnapshot(`
          [
            {
              "oldRow": {
                "created": 400,
                "id": "i4",
                "text": "d",
              },
              "row": {
                "created": 400,
                "id": "i4",
                "text": "d2",
              },
              "type": "edit",
            },
          ]
        `);
      });
    });

    describe('at limit', () => {
      test('edit row after boundary', () => {
        const {data, messages, storage, pushes} = takeNoPartitionTest({
          ...base,
          limit: 3,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'i4', created: 400, text: 'd'},
              row: {id: 'i4', created: 400, text: 'd2'},
            },
          ],
        });
        expect(data).toMatchInlineSnapshot(`
          [
            {
              "created": 100,
              "id": "i1",
              "text": "a",
              Symbol(rc): 1,
            },
            {
              "created": 200,
              "id": "i2",
              "text": "b",
              Symbol(rc): 1,
            },
            {
              "created": 300,
              "id": "i3",
              "text": "c",
              Symbol(rc): 1,
            },
          ]
        `);
        expect(messages).toMatchInlineSnapshot(`
          [
            [
              ":source(testTable)",
              "push",
              {
                "oldRow": {
                  "created": 400,
                  "id": "i4",
                  "text": "d",
                },
                "row": {
                  "created": 400,
                  "id": "i4",
                  "text": "d2",
                },
                "type": "edit",
              },
            ],
          ]
        `);
        expect(storage).toMatchInlineSnapshot(`
          {
            "["take"]": {
              "bound": {
                "created": 300,
                "id": "i3",
                "text": "c",
              },
              "size": 3,
            },
            "maxBound": {
              "created": 300,
              "id": "i3",
              "text": "c",
            },
          }
        `);
        expect(pushes).toMatchInlineSnapshot(`[]`);
      });

      test('edit row before boundary', () => {
        const {data, messages, storage, pushes} = takeNoPartitionTest({
          ...base,
          limit: 3,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'i2', created: 200, text: 'b'},
              row: {id: 'i2', created: 200, text: 'b2'},
            },
          ],
        });
        expect(data).toMatchInlineSnapshot(`
          [
            {
              "created": 100,
              "id": "i1",
              "text": "a",
              Symbol(rc): 1,
            },
            {
              "created": 200,
              "id": "i2",
              "text": "b2",
              Symbol(rc): 1,
            },
            {
              "created": 300,
              "id": "i3",
              "text": "c",
              Symbol(rc): 1,
            },
          ]
        `);
        expect(messages).toMatchInlineSnapshot(`
          [
            [
              ":source(testTable)",
              "push",
              {
                "oldRow": {
                  "created": 200,
                  "id": "i2",
                  "text": "b",
                },
                "row": {
                  "created": 200,
                  "id": "i2",
                  "text": "b2",
                },
                "type": "edit",
              },
            ],
            [
              ":take",
              "push",
              {
                "oldRow": {
                  "created": 200,
                  "id": "i2",
                  "text": "b",
                },
                "row": {
                  "created": 200,
                  "id": "i2",
                  "text": "b2",
                },
                "type": "edit",
              },
            ],
          ]
        `);
        expect(storage).toMatchInlineSnapshot(`
          {
            "["take"]": {
              "bound": {
                "created": 300,
                "id": "i3",
                "text": "c",
              },
              "size": 3,
            },
            "maxBound": {
              "created": 300,
              "id": "i3",
              "text": "c",
            },
          }
        `);
        expect(pushes).toMatchInlineSnapshot(`
          [
            {
              "oldRow": {
                "created": 200,
                "id": "i2",
                "text": "b",
              },
              "row": {
                "created": 200,
                "id": "i2",
                "text": "b2",
              },
              "type": "edit",
            },
          ]
        `);
      });

      test('edit row at boundary', () => {
        const {data, messages, storage, pushes} = takeNoPartitionTest({
          ...base,
          limit: 3,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'i3', created: 300, text: 'c'},
              row: {id: 'i3', created: 300, text: 'c2'},
            },
          ],
        });
        expect(data).toMatchInlineSnapshot(`
          [
            {
              "created": 100,
              "id": "i1",
              "text": "a",
              Symbol(rc): 1,
            },
            {
              "created": 200,
              "id": "i2",
              "text": "b",
              Symbol(rc): 1,
            },
            {
              "created": 300,
              "id": "i3",
              "text": "c2",
              Symbol(rc): 1,
            },
          ]
        `);
        expect(messages).toMatchInlineSnapshot(`
          [
            [
              ":source(testTable)",
              "push",
              {
                "oldRow": {
                  "created": 300,
                  "id": "i3",
                  "text": "c",
                },
                "row": {
                  "created": 300,
                  "id": "i3",
                  "text": "c2",
                },
                "type": "edit",
              },
            ],
            [
              ":take",
              "push",
              {
                "oldRow": {
                  "created": 300,
                  "id": "i3",
                  "text": "c",
                },
                "row": {
                  "created": 300,
                  "id": "i3",
                  "text": "c2",
                },
                "type": "edit",
              },
            ],
          ]
        `);
        expect(storage).toMatchInlineSnapshot(`
          {
            "["take"]": {
              "bound": {
                "created": 300,
                "id": "i3",
                "text": "c",
              },
              "size": 3,
            },
            "maxBound": {
              "created": 300,
              "id": "i3",
              "text": "c",
            },
          }
        `);
        expect(pushes).toMatchInlineSnapshot(`
          [
            {
              "oldRow": {
                "created": 300,
                "id": "i3",
                "text": "c",
              },
              "row": {
                "created": 300,
                "id": "i3",
                "text": "c2",
              },
              "type": "edit",
            },
          ]
        `);
      });

      test('edit row before boundary, changing its order', () => {
        const {data, messages, storage, pushes} = takeNoPartitionTest({
          ...base,
          limit: 3,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'i2', created: 200, text: 'b'},
              row: {id: 'i2', created: 50, text: 'b2'},
            },
          ],
        });
        expect(data).toMatchInlineSnapshot(`
          [
            {
              "created": 50,
              "id": "i2",
              "text": "b2",
              Symbol(rc): 1,
            },
            {
              "created": 100,
              "id": "i1",
              "text": "a",
              Symbol(rc): 1,
            },
            {
              "created": 300,
              "id": "i3",
              "text": "c",
              Symbol(rc): 1,
            },
          ]
        `);
        expect(messages).toMatchInlineSnapshot(`
          [
            [
              ":source(testTable)",
              "push",
              {
                "oldRow": {
                  "created": 200,
                  "id": "i2",
                  "text": "b",
                },
                "row": {
                  "created": 50,
                  "id": "i2",
                  "text": "b2",
                },
                "type": "edit",
              },
            ],
            [
              ":take",
              "push",
              {
                "oldRow": {
                  "created": 200,
                  "id": "i2",
                  "text": "b",
                },
                "row": {
                  "created": 50,
                  "id": "i2",
                  "text": "b2",
                },
                "type": "edit",
              },
            ],
          ]
        `);
        expect(storage).toMatchInlineSnapshot(`
          {
            "["take"]": {
              "bound": {
                "created": 300,
                "id": "i3",
                "text": "c",
              },
              "size": 3,
            },
            "maxBound": {
              "created": 300,
              "id": "i3",
              "text": "c",
            },
          }
        `);
        expect(pushes).toMatchInlineSnapshot(`
          [
            {
              "oldRow": {
                "created": 200,
                "id": "i2",
                "text": "b",
              },
              "row": {
                "created": 50,
                "id": "i2",
                "text": "b2",
              },
              "type": "edit",
            },
          ]
        `);
      });

      test('edit row after boundary to make it the new boundary', () => {
        const {data, messages, storage, pushesWithFetch} = takeNoPartitionTest({
          ...base,
          limit: 3,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'i4', created: 400, text: 'd'},
              row: {id: 'i4', created: 250, text: 'd'},
            },
          ],
          fetchOnPush: true,
        });
        expect(data).toMatchInlineSnapshot(`
          [
            {
              "created": 100,
              "id": "i1",
              "text": "a",
              Symbol(rc): 1,
            },
            {
              "created": 200,
              "id": "i2",
              "text": "b",
              Symbol(rc): 1,
            },
            {
              "created": 250,
              "id": "i4",
              "text": "d",
              Symbol(rc): 1,
            },
          ]
        `);
        expect(messages).toMatchInlineSnapshot(`
          [
            [
              ":source(testTable)",
              "push",
              {
                "oldRow": {
                  "created": 400,
                  "id": "i4",
                  "text": "d",
                },
                "row": {
                  "created": 250,
                  "id": "i4",
                  "text": "d",
                },
                "type": "edit",
              },
            ],
            [
              ":source(testTable)",
              "fetch",
              {
                "constraint": undefined,
                "reverse": true,
                "start": {
                  "basis": "at",
                  "row": {
                    "created": 300,
                    "id": "i3",
                    "text": "c",
                  },
                },
              },
            ],
            [
              ":take",
              "push",
              {
                "row": {
                  "created": 300,
                  "id": "i3",
                  "text": "c",
                },
                "type": "remove",
              },
            ],
            [
              ":take",
              "push",
              {
                "row": {
                  "created": 250,
                  "id": "i4",
                  "text": "d",
                },
                "type": "add",
              },
            ],
          ]
        `);
        expect(storage).toMatchInlineSnapshot(`
          {
            "["take"]": {
              "bound": {
                "created": 250,
                "id": "i4",
                "text": "d",
              },
              "size": 3,
            },
            "maxBound": {
              "created": 300,
              "id": "i3",
              "text": "c",
            },
          }
        `);
        expect(pushesWithFetch).toMatchInlineSnapshot(`
          [
            {
              "change": {
                "node": {
                  "relationships": {},
                  "row": {
                    "created": 300,
                    "id": "i3",
                    "text": "c",
                  },
                },
                "type": "remove",
              },
              "fetch": [
                {
                  "relationships": {},
                  "row": {
                    "created": 100,
                    "id": "i1",
                    "text": "a",
                  },
                },
                {
                  "relationships": {},
                  "row": {
                    "created": 200,
                    "id": "i2",
                    "text": "b",
                  },
                },
              ],
            },
            {
              "change": {
                "node": {
                  "relationships": {},
                  "row": {
                    "created": 250,
                    "id": "i4",
                    "text": "d",
                  },
                },
                "type": "add",
              },
              "fetch": [
                {
                  "relationships": {},
                  "row": {
                    "created": 100,
                    "id": "i1",
                    "text": "a",
                  },
                },
                {
                  "relationships": {},
                  "row": {
                    "created": 200,
                    "id": "i2",
                    "text": "b",
                  },
                },
                {
                  "relationships": {},
                  "row": {
                    "created": 250,
                    "id": "i4",
                    "text": "d",
                  },
                },
              ],
            },
          ]
        `);
      });

      test('edit row before boundary to make it new boundary', () => {
        const {data, messages, storage, pushes} = takeNoPartitionTest({
          ...base,
          limit: 3,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'i2', created: 200, text: 'b'},
              row: {id: 'i2', created: 350, text: 'b2'},
            },
          ],
        });
        expect(data).toMatchInlineSnapshot(`
          [
            {
              "created": 100,
              "id": "i1",
              "text": "a",
              Symbol(rc): 1,
            },
            {
              "created": 300,
              "id": "i3",
              "text": "c",
              Symbol(rc): 1,
            },
            {
              "created": 350,
              "id": "i2",
              "text": "b2",
              Symbol(rc): 1,
            },
          ]
        `);
        expect(messages).toMatchInlineSnapshot(`
          [
            [
              ":source(testTable)",
              "push",
              {
                "oldRow": {
                  "created": 200,
                  "id": "i2",
                  "text": "b",
                },
                "row": {
                  "created": 350,
                  "id": "i2",
                  "text": "b2",
                },
                "type": "edit",
              },
            ],
            [
              ":source(testTable)",
              "fetch",
              {
                "constraint": undefined,
                "start": {
                  "basis": "after",
                  "row": {
                    "created": 300,
                    "id": "i3",
                    "text": "c",
                  },
                },
              },
            ],
            [
              ":take",
              "push",
              {
                "oldRow": {
                  "created": 200,
                  "id": "i2",
                  "text": "b",
                },
                "row": {
                  "created": 350,
                  "id": "i2",
                  "text": "b2",
                },
                "type": "edit",
              },
            ],
          ]
        `);
        expect(storage).toMatchInlineSnapshot(`
          {
            "["take"]": {
              "bound": {
                "created": 350,
                "id": "i2",
                "text": "b2",
              },
              "size": 3,
            },
            "maxBound": {
              "created": 350,
              "id": "i2",
              "text": "b2",
            },
          }
        `);
        expect(pushes).toMatchInlineSnapshot(`
          [
            {
              "oldRow": {
                "created": 200,
                "id": "i2",
                "text": "b",
              },
              "row": {
                "created": 350,
                "id": "i2",
                "text": "b2",
              },
              "type": "edit",
            },
          ]
        `);
      });

      test('edit row before boundary to fetch new boundary', () => {
        const {data, messages, storage, pushesWithFetch} = takeNoPartitionTest({
          ...base,
          limit: 3,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'i2', created: 200, text: 'b'},
              row: {id: 'i2', created: 450, text: 'b2'},
            },
          ],
          fetchOnPush: true,
        });
        expect(data).toMatchInlineSnapshot(`
          [
            {
              "created": 100,
              "id": "i1",
              "text": "a",
              Symbol(rc): 1,
            },
            {
              "created": 300,
              "id": "i3",
              "text": "c",
              Symbol(rc): 1,
            },
            {
              "created": 400,
              "id": "i4",
              "text": "d",
              Symbol(rc): 1,
            },
          ]
        `);
        expect(messages).toMatchInlineSnapshot(`
          [
            [
              ":source(testTable)",
              "push",
              {
                "oldRow": {
                  "created": 200,
                  "id": "i2",
                  "text": "b",
                },
                "row": {
                  "created": 450,
                  "id": "i2",
                  "text": "b2",
                },
                "type": "edit",
              },
            ],
            [
              ":source(testTable)",
              "fetch",
              {
                "constraint": undefined,
                "start": {
                  "basis": "after",
                  "row": {
                    "created": 300,
                    "id": "i3",
                    "text": "c",
                  },
                },
              },
            ],
            [
              ":take",
              "push",
              {
                "row": {
                  "created": 200,
                  "id": "i2",
                  "text": "b",
                },
                "type": "remove",
              },
            ],
            [
              ":take",
              "push",
              {
                "row": {
                  "created": 400,
                  "id": "i4",
                  "text": "d",
                },
                "type": "add",
              },
            ],
          ]
        `);
        expect(storage).toMatchInlineSnapshot(`
          {
            "["take"]": {
              "bound": {
                "created": 400,
                "id": "i4",
                "text": "d",
              },
              "size": 3,
            },
            "maxBound": {
              "created": 400,
              "id": "i4",
              "text": "d",
            },
          }
        `);
        expect(pushesWithFetch).toMatchInlineSnapshot(`
          [
            {
              "change": {
                "node": {
                  "relationships": {},
                  "row": {
                    "created": 200,
                    "id": "i2",
                    "text": "b",
                  },
                },
                "type": "remove",
              },
              "fetch": [
                {
                  "relationships": {},
                  "row": {
                    "created": 100,
                    "id": "i1",
                    "text": "a",
                  },
                },
                {
                  "relationships": {},
                  "row": {
                    "created": 300,
                    "id": "i3",
                    "text": "c",
                  },
                },
              ],
            },
            {
              "change": {
                "node": {
                  "relationships": {},
                  "row": {
                    "created": 400,
                    "id": "i4",
                    "text": "d",
                  },
                },
                "type": "add",
              },
              "fetch": [
                {
                  "relationships": {},
                  "row": {
                    "created": 100,
                    "id": "i1",
                    "text": "a",
                  },
                },
                {
                  "relationships": {},
                  "row": {
                    "created": 300,
                    "id": "i3",
                    "text": "c",
                  },
                },
                {
                  "relationships": {},
                  "row": {
                    "created": 400,
                    "id": "i4",
                    "text": "d",
                  },
                },
              ],
            },
          ]
        `);
      });
    });

    test('at limit 1', () => {
      const {data, messages, storage, pushes} = takeNoPartitionTest({
        ...base,
        limit: 1,
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'i1', created: 100, text: 'a'},
            row: {id: 'i1', created: 50, text: 'a2'},
          },
        ],
      });
      expect(data).toMatchInlineSnapshot(`
        {
          "created": 50,
          "id": "i1",
          "text": "a2",
          Symbol(rc): 1,
        }
      `);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ":source(testTable)",
            "push",
            {
              "oldRow": {
                "created": 100,
                "id": "i1",
                "text": "a",
              },
              "row": {
                "created": 50,
                "id": "i1",
                "text": "a2",
              },
              "type": "edit",
            },
          ],
          [
            ":take",
            "push",
            {
              "oldRow": {
                "created": 100,
                "id": "i1",
                "text": "a",
              },
              "row": {
                "created": 50,
                "id": "i1",
                "text": "a2",
              },
              "type": "edit",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        {
          "["take"]": {
            "bound": {
              "created": 50,
              "id": "i1",
              "text": "a2",
            },
            "size": 1,
          },
          "maxBound": {
            "created": 100,
            "id": "i1",
            "text": "a",
          },
        }
      `);
      expect(pushes).toMatchInlineSnapshot(`
        [
          {
            "oldRow": {
              "created": 100,
              "id": "i1",
              "text": "a",
            },
            "row": {
              "created": 50,
              "id": "i1",
              "text": "a2",
            },
            "type": "edit",
          },
        ]
      `);
    });
  });
});

suite('take with partition', () => {
  suite('add', () => {
    test('limit 0', () => {
      const {data, messages, storage, pushes} = takeTestWithPartition({
        sourceRows: [
          {id: 'c1', issueID: 'i1', created: 100, text: null},
          {id: 'c2', issueID: 'i1', created: 200, text: null},
          {id: 'c3', issueID: 'i1', created: 300, text: null},
        ],
        limit: 0,
        pushes: [{type: 'add', row: {id: 'c6', issueID: 'i2', created: 150}}],
      });
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "comments": [],
            "id": "i1",
            Symbol(rc): 1,
          },
          {
            "comments": [],
            "id": "i2",
            Symbol(rc): 1,
          },
        ]
      `);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ".comments:source(comment)",
            "push",
            {
              "row": {
                "created": 150,
                "id": "c6",
                "issueID": "i2",
              },
              "type": "add",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`{}`);
      expect(pushes).toMatchInlineSnapshot(`[]`);
    });

    test('less than limit add row at start', () => {
      const {data, messages, storage, pushes} = takeTestWithPartition({
        sourceRows: [
          {id: 'c1', issueID: 'i1', created: 100, text: null},
          {id: 'c2', issueID: 'i1', created: 200, text: null},
          {id: 'c3', issueID: 'i1', created: 300, text: null},
          {id: 'c4', issueID: 'i2', created: 400, text: null},
          {id: 'c5', issueID: 'i2', created: 500, text: null},
        ],
        limit: 5,
        pushes: [{type: 'add', row: {id: 'c6', issueID: 'i2', created: 150}}],
      });
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "comments": [
              {
                "created": 100,
                "id": "c1",
                "issueID": "i1",
                "text": null,
                Symbol(rc): 1,
              },
              {
                "created": 200,
                "id": "c2",
                "issueID": "i1",
                "text": null,
                Symbol(rc): 1,
              },
              {
                "created": 300,
                "id": "c3",
                "issueID": "i1",
                "text": null,
                Symbol(rc): 1,
              },
            ],
            "id": "i1",
            Symbol(rc): 1,
          },
          {
            "comments": [
              {
                "created": 150,
                "id": "c6",
                "issueID": "i2",
                Symbol(rc): 1,
              },
              {
                "created": 400,
                "id": "c4",
                "issueID": "i2",
                "text": null,
                Symbol(rc): 1,
              },
              {
                "created": 500,
                "id": "c5",
                "issueID": "i2",
                "text": null,
                Symbol(rc): 1,
              },
            ],
            "id": "i2",
            Symbol(rc): 1,
          },
        ]
      `);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ".comments:source(comment)",
            "push",
            {
              "row": {
                "created": 150,
                "id": "c6",
                "issueID": "i2",
              },
              "type": "add",
            },
          ],
          [
            ".comments:take",
            "push",
            {
              "row": {
                "created": 150,
                "id": "c6",
                "issueID": "i2",
              },
              "type": "add",
            },
          ],
          [
            ":source(issue)",
            "fetch",
            {
              "constraint": {
                "id": "i2",
              },
            },
          ],
          [
            ":join(comments)",
            "push",
            {
              "child": {
                "row": {
                  "created": 150,
                  "id": "c6",
                  "issueID": "i2",
                },
                "type": "add",
              },
              "row": {
                "id": "i2",
              },
              "type": "child",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        {
          "["take","i1"]": {
            "bound": {
              "created": 300,
              "id": "c3",
              "issueID": "i1",
              "text": null,
            },
            "size": 3,
          },
          "["take","i2"]": {
            "bound": {
              "created": 500,
              "id": "c5",
              "issueID": "i2",
              "text": null,
            },
            "size": 3,
          },
          "maxBound": {
            "created": 500,
            "id": "c5",
            "issueID": "i2",
            "text": null,
          },
        }
      `);
      expect(pushes).toMatchInlineSnapshot(`
        [
          {
            "child": {
              "change": {
                "node": {
                  "relationships": {},
                  "row": {
                    "created": 150,
                    "id": "c6",
                    "issueID": "i2",
                  },
                },
                "type": "add",
              },
              "relationshipName": "comments",
            },
            "row": {
              "id": "i2",
            },
            "type": "child",
          },
        ]
      `);
    });

    test('at limit add row at end', () => {
      const {data, messages, storage, pushesWithFetch} = takeTestWithPartition({
        sourceRows: [
          {id: 'c1', issueID: 'i1', created: 100, text: null},
          {id: 'c2', issueID: 'i1', created: 200, text: null},
          // 580 to test that it constrains looking for previous
          // to constraint issueID: 'i2'
          {id: 'c3', issueID: 'i1', created: 580, text: null},
          {id: 'c4', issueID: 'i2', created: 400, text: null},
          {id: 'c5', issueID: 'i2', created: 500, text: null},
          {id: 'c6', issueID: 'i2', created: 600, text: null},
          {id: 'c7', issueID: 'i2', created: 700, text: null},
        ],
        limit: 3,
        pushes: [{type: 'add', row: {id: 'c8', issueID: 'i2', created: 550}}],
        fetchOnPush: true,
      });
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "comments": [
              {
                "created": 100,
                "id": "c1",
                "issueID": "i1",
                "text": null,
                Symbol(rc): 1,
              },
              {
                "created": 200,
                "id": "c2",
                "issueID": "i1",
                "text": null,
                Symbol(rc): 1,
              },
              {
                "created": 580,
                "id": "c3",
                "issueID": "i1",
                "text": null,
                Symbol(rc): 1,
              },
            ],
            "id": "i1",
            Symbol(rc): 1,
          },
          {
            "comments": [
              {
                "created": 400,
                "id": "c4",
                "issueID": "i2",
                "text": null,
                Symbol(rc): 1,
              },
              {
                "created": 500,
                "id": "c5",
                "issueID": "i2",
                "text": null,
                Symbol(rc): 1,
              },
              {
                "created": 550,
                "id": "c8",
                "issueID": "i2",
                Symbol(rc): 1,
              },
            ],
            "id": "i2",
            Symbol(rc): 1,
          },
        ]
      `);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ".comments:source(comment)",
            "push",
            {
              "row": {
                "created": 550,
                "id": "c8",
                "issueID": "i2",
              },
              "type": "add",
            },
          ],
          [
            ".comments:source(comment)",
            "fetch",
            {
              "constraint": {
                "issueID": "i2",
              },
              "reverse": true,
              "start": {
                "basis": "at",
                "row": {
                  "created": 600,
                  "id": "c6",
                  "issueID": "i2",
                  "text": null,
                },
              },
            },
          ],
          [
            ".comments:take",
            "push",
            {
              "row": {
                "created": 600,
                "id": "c6",
                "issueID": "i2",
                "text": null,
              },
              "type": "remove",
            },
          ],
          [
            ":source(issue)",
            "fetch",
            {
              "constraint": {
                "id": "i2",
              },
            },
          ],
          [
            ":join(comments)",
            "push",
            {
              "child": {
                "row": {
                  "created": 600,
                  "id": "c6",
                  "issueID": "i2",
                  "text": null,
                },
                "type": "remove",
              },
              "row": {
                "id": "i2",
              },
              "type": "child",
            },
          ],
          [
            ".comments:take",
            "push",
            {
              "row": {
                "created": 550,
                "id": "c8",
                "issueID": "i2",
              },
              "type": "add",
            },
          ],
          [
            ":source(issue)",
            "fetch",
            {
              "constraint": {
                "id": "i2",
              },
            },
          ],
          [
            ":join(comments)",
            "push",
            {
              "child": {
                "row": {
                  "created": 550,
                  "id": "c8",
                  "issueID": "i2",
                },
                "type": "add",
              },
              "row": {
                "id": "i2",
              },
              "type": "child",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        {
          "["take","i1"]": {
            "bound": {
              "created": 580,
              "id": "c3",
              "issueID": "i1",
              "text": null,
            },
            "size": 3,
          },
          "["take","i2"]": {
            "bound": {
              "created": 550,
              "id": "c8",
              "issueID": "i2",
            },
            "size": 3,
          },
          "maxBound": {
            "created": 600,
            "id": "c6",
            "issueID": "i2",
            "text": null,
          },
        }
      `);
      expect(pushesWithFetch).toMatchInlineSnapshot(`
        [
          {
            "change": {
              "child": {
                "change": {
                  "node": {
                    "relationships": {},
                    "row": {
                      "created": 600,
                      "id": "c6",
                      "issueID": "i2",
                      "text": null,
                    },
                  },
                  "type": "remove",
                },
                "relationshipName": "comments",
              },
              "row": {
                "id": "i2",
              },
              "type": "child",
            },
            "fetch": [
              {
                "relationships": {
                  "comments": [
                    {
                      "relationships": {},
                      "row": {
                        "created": 100,
                        "id": "c1",
                        "issueID": "i1",
                        "text": null,
                      },
                    },
                    {
                      "relationships": {},
                      "row": {
                        "created": 200,
                        "id": "c2",
                        "issueID": "i1",
                        "text": null,
                      },
                    },
                    {
                      "relationships": {},
                      "row": {
                        "created": 580,
                        "id": "c3",
                        "issueID": "i1",
                        "text": null,
                      },
                    },
                  ],
                },
                "row": {
                  "id": "i1",
                },
              },
              {
                "relationships": {
                  "comments": [
                    {
                      "relationships": {},
                      "row": {
                        "created": 400,
                        "id": "c4",
                        "issueID": "i2",
                        "text": null,
                      },
                    },
                    {
                      "relationships": {},
                      "row": {
                        "created": 500,
                        "id": "c5",
                        "issueID": "i2",
                        "text": null,
                      },
                    },
                  ],
                },
                "row": {
                  "id": "i2",
                },
              },
            ],
          },
          {
            "change": {
              "child": {
                "change": {
                  "node": {
                    "relationships": {},
                    "row": {
                      "created": 550,
                      "id": "c8",
                      "issueID": "i2",
                    },
                  },
                  "type": "add",
                },
                "relationshipName": "comments",
              },
              "row": {
                "id": "i2",
              },
              "type": "child",
            },
            "fetch": [
              {
                "relationships": {
                  "comments": [
                    {
                      "relationships": {},
                      "row": {
                        "created": 100,
                        "id": "c1",
                        "issueID": "i1",
                        "text": null,
                      },
                    },
                    {
                      "relationships": {},
                      "row": {
                        "created": 200,
                        "id": "c2",
                        "issueID": "i1",
                        "text": null,
                      },
                    },
                    {
                      "relationships": {},
                      "row": {
                        "created": 580,
                        "id": "c3",
                        "issueID": "i1",
                        "text": null,
                      },
                    },
                  ],
                },
                "row": {
                  "id": "i1",
                },
              },
              {
                "relationships": {
                  "comments": [
                    {
                      "relationships": {},
                      "row": {
                        "created": 400,
                        "id": "c4",
                        "issueID": "i2",
                        "text": null,
                      },
                    },
                    {
                      "relationships": {},
                      "row": {
                        "created": 500,
                        "id": "c5",
                        "issueID": "i2",
                        "text": null,
                      },
                    },
                    {
                      "relationships": {},
                      "row": {
                        "created": 550,
                        "id": "c8",
                        "issueID": "i2",
                      },
                    },
                  ],
                },
                "row": {
                  "id": "i2",
                },
              },
            ],
          },
        ]
      `);
    });

    test('add with non-fetched partition value', () => {
      const {data, messages, storage, pushes} = takeTestWithPartition({
        sourceRows: [
          {id: 'c1', issueID: 'i1', created: 100, text: null},
          {id: 'c2', issueID: 'i1', created: 200, text: null},
          {id: 'c3', issueID: 'i1', created: 300, text: null},
          {id: 'c4', issueID: 'i2', created: 400, text: null},
          {id: 'c5', issueID: 'i2', created: 500, text: null},
        ],
        limit: 3,
        pushes: [{type: 'add', row: {id: 'c6', issueID: '3', created: 550}}],
      });
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "comments": [
              {
                "created": 100,
                "id": "c1",
                "issueID": "i1",
                "text": null,
                Symbol(rc): 1,
              },
              {
                "created": 200,
                "id": "c2",
                "issueID": "i1",
                "text": null,
                Symbol(rc): 1,
              },
              {
                "created": 300,
                "id": "c3",
                "issueID": "i1",
                "text": null,
                Symbol(rc): 1,
              },
            ],
            "id": "i1",
            Symbol(rc): 1,
          },
          {
            "comments": [
              {
                "created": 400,
                "id": "c4",
                "issueID": "i2",
                "text": null,
                Symbol(rc): 1,
              },
              {
                "created": 500,
                "id": "c5",
                "issueID": "i2",
                "text": null,
                Symbol(rc): 1,
              },
            ],
            "id": "i2",
            Symbol(rc): 1,
          },
        ]
      `);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ".comments:source(comment)",
            "push",
            {
              "row": {
                "created": 550,
                "id": "c6",
                "issueID": "3",
              },
              "type": "add",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        {
          "["take","i1"]": {
            "bound": {
              "created": 300,
              "id": "c3",
              "issueID": "i1",
              "text": null,
            },
            "size": 3,
          },
          "["take","i2"]": {
            "bound": {
              "created": 500,
              "id": "c5",
              "issueID": "i2",
              "text": null,
            },
            "size": 2,
          },
          "maxBound": {
            "created": 500,
            "id": "c5",
            "issueID": "i2",
            "text": null,
          },
        }
      `);
      expect(pushes).toMatchInlineSnapshot(`[]`);
    });
  });

  suite('remove', () => {
    test('limit 0', () => {
      const {data, messages, storage, pushes} = takeTestWithPartition({
        sourceRows: [
          {id: 'c1', issueID: 'i1', created: 100, text: null},
          {id: 'c2', issueID: 'i1', created: 200, text: null},
          {id: 'c3', issueID: 'i1', created: 300, text: null},
        ],
        limit: 0,
        pushes: [
          {type: 'remove', row: {id: 'c1', issueID: 'i1', created: 100}},
        ],
      });
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "comments": [],
            "id": "i1",
            Symbol(rc): 1,
          },
          {
            "comments": [],
            "id": "i2",
            Symbol(rc): 1,
          },
        ]
      `);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ".comments:source(comment)",
            "push",
            {
              "row": {
                "created": 100,
                "id": "c1",
                "issueID": "i1",
              },
              "type": "remove",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`{}`);
      expect(pushes).toMatchInlineSnapshot(`[]`);
    });

    test('less than limit remove row at start', () => {
      const {data, messages, storage, pushes} = takeTestWithPartition({
        sourceRows: [
          {id: 'c1', issueID: 'i1', created: 100, text: null},
          {id: 'c2', issueID: 'i1', created: 200, text: null},
          {id: 'c3', issueID: 'i1', created: 300, text: null},
          {id: 'c4', issueID: 'i2', created: 400, text: null},
          {id: 'c5', issueID: 'i2', created: 500, text: null},
        ],
        limit: 5,
        pushes: [
          {type: 'remove', row: {id: 'c1', issueID: 'i1', created: 100}},
        ],
      });
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "comments": [
              {
                "created": 200,
                "id": "c2",
                "issueID": "i1",
                "text": null,
                Symbol(rc): 1,
              },
              {
                "created": 300,
                "id": "c3",
                "issueID": "i1",
                "text": null,
                Symbol(rc): 1,
              },
            ],
            "id": "i1",
            Symbol(rc): 1,
          },
          {
            "comments": [
              {
                "created": 400,
                "id": "c4",
                "issueID": "i2",
                "text": null,
                Symbol(rc): 1,
              },
              {
                "created": 500,
                "id": "c5",
                "issueID": "i2",
                "text": null,
                Symbol(rc): 1,
              },
            ],
            "id": "i2",
            Symbol(rc): 1,
          },
        ]
      `);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ".comments:source(comment)",
            "push",
            {
              "row": {
                "created": 100,
                "id": "c1",
                "issueID": "i1",
              },
              "type": "remove",
            },
          ],
          [
            ".comments:source(comment)",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
              "reverse": true,
              "start": {
                "basis": "after",
                "row": {
                  "created": 300,
                  "id": "c3",
                  "issueID": "i1",
                  "text": null,
                },
              },
            },
          ],
          [
            ".comments:source(comment)",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
              "start": {
                "basis": "at",
                "row": {
                  "created": 300,
                  "id": "c3",
                  "issueID": "i1",
                  "text": null,
                },
              },
            },
          ],
          [
            ".comments:take",
            "push",
            {
              "row": {
                "created": 100,
                "id": "c1",
                "issueID": "i1",
              },
              "type": "remove",
            },
          ],
          [
            ":source(issue)",
            "fetch",
            {
              "constraint": {
                "id": "i1",
              },
            },
          ],
          [
            ":join(comments)",
            "push",
            {
              "child": {
                "row": {
                  "created": 100,
                  "id": "c1",
                  "issueID": "i1",
                },
                "type": "remove",
              },
              "row": {
                "id": "i1",
              },
              "type": "child",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        {
          "["take","i1"]": {
            "bound": {
              "created": 300,
              "id": "c3",
              "issueID": "i1",
              "text": null,
            },
            "size": 2,
          },
          "["take","i2"]": {
            "bound": {
              "created": 500,
              "id": "c5",
              "issueID": "i2",
              "text": null,
            },
            "size": 2,
          },
          "maxBound": {
            "created": 500,
            "id": "c5",
            "issueID": "i2",
            "text": null,
          },
        }
      `);
      expect(pushes).toMatchInlineSnapshot(`
        [
          {
            "child": {
              "change": {
                "node": {
                  "relationships": {},
                  "row": {
                    "created": 100,
                    "id": "c1",
                    "issueID": "i1",
                  },
                },
                "type": "remove",
              },
              "relationshipName": "comments",
            },
            "row": {
              "id": "i1",
            },
            "type": "child",
          },
        ]
      `);
    });

    test('remove row unfetched partition', () => {
      const {data, messages, storage, pushes} = takeTestWithPartition({
        sourceRows: [
          {id: 'c1', issueID: 'i1', created: 100, text: null},
          {id: 'c2', issueID: 'i1', created: 200, text: null},
          {id: 'c3', issueID: 'i1', created: 300, text: null},
          {id: 'c4', issueID: 'i2', created: 400, text: null},
          {id: 'c5', issueID: 'i2', created: 500, text: null},
          {id: 'c6', issueID: 'i3', created: 600, text: null},
        ],
        limit: 5,
        pushes: [
          {type: 'remove', row: {id: 'c6', issueID: 'i3', created: 600}},
        ],
      });
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "comments": [
              {
                "created": 100,
                "id": "c1",
                "issueID": "i1",
                "text": null,
                Symbol(rc): 1,
              },
              {
                "created": 200,
                "id": "c2",
                "issueID": "i1",
                "text": null,
                Symbol(rc): 1,
              },
              {
                "created": 300,
                "id": "c3",
                "issueID": "i1",
                "text": null,
                Symbol(rc): 1,
              },
            ],
            "id": "i1",
            Symbol(rc): 1,
          },
          {
            "comments": [
              {
                "created": 400,
                "id": "c4",
                "issueID": "i2",
                "text": null,
                Symbol(rc): 1,
              },
              {
                "created": 500,
                "id": "c5",
                "issueID": "i2",
                "text": null,
                Symbol(rc): 1,
              },
            ],
            "id": "i2",
            Symbol(rc): 1,
          },
        ]
      `);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ".comments:source(comment)",
            "push",
            {
              "row": {
                "created": 600,
                "id": "c6",
                "issueID": "i3",
              },
              "type": "remove",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        {
          "["take","i1"]": {
            "bound": {
              "created": 300,
              "id": "c3",
              "issueID": "i1",
              "text": null,
            },
            "size": 3,
          },
          "["take","i2"]": {
            "bound": {
              "created": 500,
              "id": "c5",
              "issueID": "i2",
              "text": null,
            },
            "size": 2,
          },
          "maxBound": {
            "created": 500,
            "id": "c5",
            "issueID": "i2",
            "text": null,
          },
        }
      `);
      expect(pushes).toMatchInlineSnapshot(`[]`);
    });
  });

  suite('edit', () => {
    const base = {
      sourceRows: [
        {id: 'c1', issueID: 'i1', created: 100, text: 'a'},
        {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
        {id: 'c3', issueID: 'i1', created: 300, text: 'c'},
        {id: 'c4', issueID: 'i2', created: 400, text: 'd'},
        {id: 'c5', issueID: 'i2', created: 500, text: 'e'},
      ],
    } as const;

    test('limit 0', () => {
      const {data, messages, storage, pushes} = takeTestWithPartition({
        ...base,
        limit: 0,
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
            row: {id: 'c2', issueID: 'i1', created: 200, text: 'b2'},
          },
        ],
      });
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "comments": [],
            "id": "i1",
            Symbol(rc): 1,
          },
          {
            "comments": [],
            "id": "i2",
            Symbol(rc): 1,
          },
        ]
      `);
      expect(messages).toMatchInlineSnapshot(`
        [
          [
            ".comments:source(comment)",
            "push",
            {
              "oldRow": {
                "created": 200,
                "id": "c2",
                "issueID": "i1",
                "text": "b",
              },
              "row": {
                "created": 200,
                "id": "c2",
                "issueID": "i1",
                "text": "b2",
              },
              "type": "edit",
            },
          ],
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`{}`);
      expect(pushes).toMatchInlineSnapshot(`[]`);
    });

    describe('less than limit ', () => {
      test('edit row at start', () => {
        const {data, messages, storage, pushes} = takeTestWithPartition({
          ...base,
          limit: 5,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'c1', issueID: 'i1', created: 100, text: 'a'},
              row: {id: 'c1', issueID: 'i1', created: 100, text: 'a2'},
            },
          ],
        });
        expect(data).toMatchInlineSnapshot(`
          [
            {
              "comments": [
                {
                  "created": 100,
                  "id": "c1",
                  "issueID": "i1",
                  "text": "a2",
                  Symbol(rc): 1,
                },
                {
                  "created": 200,
                  "id": "c2",
                  "issueID": "i1",
                  "text": "b",
                  Symbol(rc): 1,
                },
                {
                  "created": 300,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c",
                  Symbol(rc): 1,
                },
              ],
              "id": "i1",
              Symbol(rc): 1,
            },
            {
              "comments": [
                {
                  "created": 400,
                  "id": "c4",
                  "issueID": "i2",
                  "text": "d",
                  Symbol(rc): 1,
                },
                {
                  "created": 500,
                  "id": "c5",
                  "issueID": "i2",
                  "text": "e",
                  Symbol(rc): 1,
                },
              ],
              "id": "i2",
              Symbol(rc): 1,
            },
          ]
        `);
        expect(messages).toMatchInlineSnapshot(`
          [
            [
              ".comments:source(comment)",
              "push",
              {
                "oldRow": {
                  "created": 100,
                  "id": "c1",
                  "issueID": "i1",
                  "text": "a",
                },
                "row": {
                  "created": 100,
                  "id": "c1",
                  "issueID": "i1",
                  "text": "a2",
                },
                "type": "edit",
              },
            ],
            [
              ".comments:take",
              "push",
              {
                "oldRow": {
                  "created": 100,
                  "id": "c1",
                  "issueID": "i1",
                  "text": "a",
                },
                "row": {
                  "created": 100,
                  "id": "c1",
                  "issueID": "i1",
                  "text": "a2",
                },
                "type": "edit",
              },
            ],
            [
              ":source(issue)",
              "fetch",
              {
                "constraint": {
                  "id": "i1",
                },
              },
            ],
            [
              ":join(comments)",
              "push",
              {
                "child": {
                  "oldRow": {
                    "created": 100,
                    "id": "c1",
                    "issueID": "i1",
                    "text": "a",
                  },
                  "row": {
                    "created": 100,
                    "id": "c1",
                    "issueID": "i1",
                    "text": "a2",
                  },
                  "type": "edit",
                },
                "row": {
                  "id": "i1",
                },
                "type": "child",
              },
            ],
          ]
        `);
        expect(storage).toMatchInlineSnapshot(`
          {
            "["take","i1"]": {
              "bound": {
                "created": 300,
                "id": "c3",
                "issueID": "i1",
                "text": "c",
              },
              "size": 3,
            },
            "["take","i2"]": {
              "bound": {
                "created": 500,
                "id": "c5",
                "issueID": "i2",
                "text": "e",
              },
              "size": 2,
            },
            "maxBound": {
              "created": 500,
              "id": "c5",
              "issueID": "i2",
              "text": "e",
            },
          }
        `);
        expect(pushes).toMatchInlineSnapshot(`
          [
            {
              "child": {
                "change": {
                  "oldRow": {
                    "created": 100,
                    "id": "c1",
                    "issueID": "i1",
                    "text": "a",
                  },
                  "row": {
                    "created": 100,
                    "id": "c1",
                    "issueID": "i1",
                    "text": "a2",
                  },
                  "type": "edit",
                },
                "relationshipName": "comments",
              },
              "row": {
                "id": "i1",
              },
              "type": "child",
            },
          ]
        `);
      });

      test('edit row at end', () => {
        const {data, messages, storage, pushes} = takeTestWithPartition({
          ...base,
          limit: 5,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'c5', issueID: 'i2', created: 500, text: 'e'},
              row: {id: 'c5', issueID: 'i2', created: 500, text: 'e2'},
            },
          ],
        });
        expect(data).toMatchInlineSnapshot(`
          [
            {
              "comments": [
                {
                  "created": 100,
                  "id": "c1",
                  "issueID": "i1",
                  "text": "a",
                  Symbol(rc): 1,
                },
                {
                  "created": 200,
                  "id": "c2",
                  "issueID": "i1",
                  "text": "b",
                  Symbol(rc): 1,
                },
                {
                  "created": 300,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c",
                  Symbol(rc): 1,
                },
              ],
              "id": "i1",
              Symbol(rc): 1,
            },
            {
              "comments": [
                {
                  "created": 400,
                  "id": "c4",
                  "issueID": "i2",
                  "text": "d",
                  Symbol(rc): 1,
                },
                {
                  "created": 500,
                  "id": "c5",
                  "issueID": "i2",
                  "text": "e2",
                  Symbol(rc): 1,
                },
              ],
              "id": "i2",
              Symbol(rc): 1,
            },
          ]
        `);
        expect(messages).toMatchInlineSnapshot(`
          [
            [
              ".comments:source(comment)",
              "push",
              {
                "oldRow": {
                  "created": 500,
                  "id": "c5",
                  "issueID": "i2",
                  "text": "e",
                },
                "row": {
                  "created": 500,
                  "id": "c5",
                  "issueID": "i2",
                  "text": "e2",
                },
                "type": "edit",
              },
            ],
            [
              ".comments:take",
              "push",
              {
                "oldRow": {
                  "created": 500,
                  "id": "c5",
                  "issueID": "i2",
                  "text": "e",
                },
                "row": {
                  "created": 500,
                  "id": "c5",
                  "issueID": "i2",
                  "text": "e2",
                },
                "type": "edit",
              },
            ],
            [
              ":source(issue)",
              "fetch",
              {
                "constraint": {
                  "id": "i2",
                },
              },
            ],
            [
              ":join(comments)",
              "push",
              {
                "child": {
                  "oldRow": {
                    "created": 500,
                    "id": "c5",
                    "issueID": "i2",
                    "text": "e",
                  },
                  "row": {
                    "created": 500,
                    "id": "c5",
                    "issueID": "i2",
                    "text": "e2",
                  },
                  "type": "edit",
                },
                "row": {
                  "id": "i2",
                },
                "type": "child",
              },
            ],
          ]
        `);
        expect(storage).toMatchInlineSnapshot(`
          {
            "["take","i1"]": {
              "bound": {
                "created": 300,
                "id": "c3",
                "issueID": "i1",
                "text": "c",
              },
              "size": 3,
            },
            "["take","i2"]": {
              "bound": {
                "created": 500,
                "id": "c5",
                "issueID": "i2",
                "text": "e",
              },
              "size": 2,
            },
            "maxBound": {
              "created": 500,
              "id": "c5",
              "issueID": "i2",
              "text": "e",
            },
          }
        `);
        expect(pushes).toMatchInlineSnapshot(`
          [
            {
              "child": {
                "change": {
                  "oldRow": {
                    "created": 500,
                    "id": "c5",
                    "issueID": "i2",
                    "text": "e",
                  },
                  "row": {
                    "created": 500,
                    "id": "c5",
                    "issueID": "i2",
                    "text": "e2",
                  },
                  "type": "edit",
                },
                "relationshipName": "comments",
              },
              "row": {
                "id": "i2",
              },
              "type": "child",
            },
          ]
        `);
      });
    });

    describe('at limit', () => {
      test('edit row after boundary', () => {
        const {data, messages, storage, pushes} = takeTestWithPartition({
          ...base,
          limit: 2,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'c3', issueID: 'i1', created: 300, text: 'c'},
              row: {id: 'c3', issueID: 'i1', created: 300, text: 'c2'},
            },
          ],
        });
        expect(data).toMatchInlineSnapshot(`
          [
            {
              "comments": [
                {
                  "created": 100,
                  "id": "c1",
                  "issueID": "i1",
                  "text": "a",
                  Symbol(rc): 1,
                },
                {
                  "created": 200,
                  "id": "c2",
                  "issueID": "i1",
                  "text": "b",
                  Symbol(rc): 1,
                },
              ],
              "id": "i1",
              Symbol(rc): 1,
            },
            {
              "comments": [
                {
                  "created": 400,
                  "id": "c4",
                  "issueID": "i2",
                  "text": "d",
                  Symbol(rc): 1,
                },
                {
                  "created": 500,
                  "id": "c5",
                  "issueID": "i2",
                  "text": "e",
                  Symbol(rc): 1,
                },
              ],
              "id": "i2",
              Symbol(rc): 1,
            },
          ]
        `);
        expect(messages).toMatchInlineSnapshot(`
          [
            [
              ".comments:source(comment)",
              "push",
              {
                "oldRow": {
                  "created": 300,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c",
                },
                "row": {
                  "created": 300,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c2",
                },
                "type": "edit",
              },
            ],
          ]
        `);
        expect(storage).toMatchInlineSnapshot(`
          {
            "["take","i1"]": {
              "bound": {
                "created": 200,
                "id": "c2",
                "issueID": "i1",
                "text": "b",
              },
              "size": 2,
            },
            "["take","i2"]": {
              "bound": {
                "created": 500,
                "id": "c5",
                "issueID": "i2",
                "text": "e",
              },
              "size": 2,
            },
            "maxBound": {
              "created": 500,
              "id": "c5",
              "issueID": "i2",
              "text": "e",
            },
          }
        `);
        expect(pushes).toMatchInlineSnapshot(`[]`);
      });

      test('edit row before boundary', () => {
        const {data, messages, storage, pushes} = takeTestWithPartition({
          ...base,
          limit: 3,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
              row: {id: 'c2', issueID: 'i1', created: 200, text: 'b2'},
            },
          ],
        });
        expect(data).toMatchInlineSnapshot(`
          [
            {
              "comments": [
                {
                  "created": 100,
                  "id": "c1",
                  "issueID": "i1",
                  "text": "a",
                  Symbol(rc): 1,
                },
                {
                  "created": 200,
                  "id": "c2",
                  "issueID": "i1",
                  "text": "b2",
                  Symbol(rc): 1,
                },
                {
                  "created": 300,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c",
                  Symbol(rc): 1,
                },
              ],
              "id": "i1",
              Symbol(rc): 1,
            },
            {
              "comments": [
                {
                  "created": 400,
                  "id": "c4",
                  "issueID": "i2",
                  "text": "d",
                  Symbol(rc): 1,
                },
                {
                  "created": 500,
                  "id": "c5",
                  "issueID": "i2",
                  "text": "e",
                  Symbol(rc): 1,
                },
              ],
              "id": "i2",
              Symbol(rc): 1,
            },
          ]
        `);
        expect(messages).toMatchInlineSnapshot(`
          [
            [
              ".comments:source(comment)",
              "push",
              {
                "oldRow": {
                  "created": 200,
                  "id": "c2",
                  "issueID": "i1",
                  "text": "b",
                },
                "row": {
                  "created": 200,
                  "id": "c2",
                  "issueID": "i1",
                  "text": "b2",
                },
                "type": "edit",
              },
            ],
            [
              ".comments:take",
              "push",
              {
                "oldRow": {
                  "created": 200,
                  "id": "c2",
                  "issueID": "i1",
                  "text": "b",
                },
                "row": {
                  "created": 200,
                  "id": "c2",
                  "issueID": "i1",
                  "text": "b2",
                },
                "type": "edit",
              },
            ],
            [
              ":source(issue)",
              "fetch",
              {
                "constraint": {
                  "id": "i1",
                },
              },
            ],
            [
              ":join(comments)",
              "push",
              {
                "child": {
                  "oldRow": {
                    "created": 200,
                    "id": "c2",
                    "issueID": "i1",
                    "text": "b",
                  },
                  "row": {
                    "created": 200,
                    "id": "c2",
                    "issueID": "i1",
                    "text": "b2",
                  },
                  "type": "edit",
                },
                "row": {
                  "id": "i1",
                },
                "type": "child",
              },
            ],
          ]
        `);
        expect(storage).toMatchInlineSnapshot(`
          {
            "["take","i1"]": {
              "bound": {
                "created": 300,
                "id": "c3",
                "issueID": "i1",
                "text": "c",
              },
              "size": 3,
            },
            "["take","i2"]": {
              "bound": {
                "created": 500,
                "id": "c5",
                "issueID": "i2",
                "text": "e",
              },
              "size": 2,
            },
            "maxBound": {
              "created": 500,
              "id": "c5",
              "issueID": "i2",
              "text": "e",
            },
          }
        `);
        expect(pushes).toMatchInlineSnapshot(`
          [
            {
              "child": {
                "change": {
                  "oldRow": {
                    "created": 200,
                    "id": "c2",
                    "issueID": "i1",
                    "text": "b",
                  },
                  "row": {
                    "created": 200,
                    "id": "c2",
                    "issueID": "i1",
                    "text": "b2",
                  },
                  "type": "edit",
                },
                "relationshipName": "comments",
              },
              "row": {
                "id": "i1",
              },
              "type": "child",
            },
          ]
        `);
      });

      test('edit row at boundary', () => {
        const {data, messages, storage, pushes} = takeTestWithPartition({
          ...base,
          limit: 3,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'c3', issueID: 'i1', created: 300, text: 'c'},
              row: {id: 'c3', issueID: 'i1', created: 300, text: 'c2'},
            },
          ],
        });
        expect(data).toMatchInlineSnapshot(`
          [
            {
              "comments": [
                {
                  "created": 100,
                  "id": "c1",
                  "issueID": "i1",
                  "text": "a",
                  Symbol(rc): 1,
                },
                {
                  "created": 200,
                  "id": "c2",
                  "issueID": "i1",
                  "text": "b",
                  Symbol(rc): 1,
                },
                {
                  "created": 300,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c2",
                  Symbol(rc): 1,
                },
              ],
              "id": "i1",
              Symbol(rc): 1,
            },
            {
              "comments": [
                {
                  "created": 400,
                  "id": "c4",
                  "issueID": "i2",
                  "text": "d",
                  Symbol(rc): 1,
                },
                {
                  "created": 500,
                  "id": "c5",
                  "issueID": "i2",
                  "text": "e",
                  Symbol(rc): 1,
                },
              ],
              "id": "i2",
              Symbol(rc): 1,
            },
          ]
        `);
        expect(messages).toMatchInlineSnapshot(`
          [
            [
              ".comments:source(comment)",
              "push",
              {
                "oldRow": {
                  "created": 300,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c",
                },
                "row": {
                  "created": 300,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c2",
                },
                "type": "edit",
              },
            ],
            [
              ".comments:take",
              "push",
              {
                "oldRow": {
                  "created": 300,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c",
                },
                "row": {
                  "created": 300,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c2",
                },
                "type": "edit",
              },
            ],
            [
              ":source(issue)",
              "fetch",
              {
                "constraint": {
                  "id": "i1",
                },
              },
            ],
            [
              ":join(comments)",
              "push",
              {
                "child": {
                  "oldRow": {
                    "created": 300,
                    "id": "c3",
                    "issueID": "i1",
                    "text": "c",
                  },
                  "row": {
                    "created": 300,
                    "id": "c3",
                    "issueID": "i1",
                    "text": "c2",
                  },
                  "type": "edit",
                },
                "row": {
                  "id": "i1",
                },
                "type": "child",
              },
            ],
          ]
        `);
        expect(storage).toMatchInlineSnapshot(`
          {
            "["take","i1"]": {
              "bound": {
                "created": 300,
                "id": "c3",
                "issueID": "i1",
                "text": "c",
              },
              "size": 3,
            },
            "["take","i2"]": {
              "bound": {
                "created": 500,
                "id": "c5",
                "issueID": "i2",
                "text": "e",
              },
              "size": 2,
            },
            "maxBound": {
              "created": 500,
              "id": "c5",
              "issueID": "i2",
              "text": "e",
            },
          }
        `);
        expect(pushes).toMatchInlineSnapshot(`
          [
            {
              "child": {
                "change": {
                  "oldRow": {
                    "created": 300,
                    "id": "c3",
                    "issueID": "i1",
                    "text": "c",
                  },
                  "row": {
                    "created": 300,
                    "id": "c3",
                    "issueID": "i1",
                    "text": "c2",
                  },
                  "type": "edit",
                },
                "relationshipName": "comments",
              },
              "row": {
                "id": "i1",
              },
              "type": "child",
            },
          ]
        `);
      });

      test('edit row at boundary, making it not the boundary', () => {
        const {data, messages, storage, pushes} = takeTestWithPartition({
          ...base,
          limit: 3,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'c3', issueID: 'i1', created: 300, text: 'c'},
              row: {id: 'c3', issueID: 'i1', created: 150, text: 'c2'},
            },
          ],
        });
        expect(data).toMatchInlineSnapshot(`
          [
            {
              "comments": [
                {
                  "created": 100,
                  "id": "c1",
                  "issueID": "i1",
                  "text": "a",
                  Symbol(rc): 1,
                },
                {
                  "created": 150,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c2",
                  Symbol(rc): 1,
                },
                {
                  "created": 200,
                  "id": "c2",
                  "issueID": "i1",
                  "text": "b",
                  Symbol(rc): 1,
                },
              ],
              "id": "i1",
              Symbol(rc): 1,
            },
            {
              "comments": [
                {
                  "created": 400,
                  "id": "c4",
                  "issueID": "i2",
                  "text": "d",
                  Symbol(rc): 1,
                },
                {
                  "created": 500,
                  "id": "c5",
                  "issueID": "i2",
                  "text": "e",
                  Symbol(rc): 1,
                },
              ],
              "id": "i2",
              Symbol(rc): 1,
            },
          ]
        `);
        expect(messages).toMatchInlineSnapshot(`
          [
            [
              ".comments:source(comment)",
              "push",
              {
                "oldRow": {
                  "created": 300,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c",
                },
                "row": {
                  "created": 150,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c2",
                },
                "type": "edit",
              },
            ],
            [
              ".comments:source(comment)",
              "fetch",
              {
                "constraint": {
                  "issueID": "i1",
                },
                "reverse": true,
                "start": {
                  "basis": "after",
                  "row": {
                    "created": 300,
                    "id": "c3",
                    "issueID": "i1",
                    "text": "c",
                  },
                },
              },
            ],
            [
              ".comments:take",
              "push",
              {
                "oldRow": {
                  "created": 300,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c",
                },
                "row": {
                  "created": 150,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c2",
                },
                "type": "edit",
              },
            ],
            [
              ":source(issue)",
              "fetch",
              {
                "constraint": {
                  "id": "i1",
                },
              },
            ],
            [
              ":join(comments)",
              "push",
              {
                "child": {
                  "oldRow": {
                    "created": 300,
                    "id": "c3",
                    "issueID": "i1",
                    "text": "c",
                  },
                  "row": {
                    "created": 150,
                    "id": "c3",
                    "issueID": "i1",
                    "text": "c2",
                  },
                  "type": "edit",
                },
                "row": {
                  "id": "i1",
                },
                "type": "child",
              },
            ],
          ]
        `);
        expect(storage).toMatchInlineSnapshot(`
          {
            "["take","i1"]": {
              "bound": {
                "created": 200,
                "id": "c2",
                "issueID": "i1",
                "text": "b",
              },
              "size": 3,
            },
            "["take","i2"]": {
              "bound": {
                "created": 500,
                "id": "c5",
                "issueID": "i2",
                "text": "e",
              },
              "size": 2,
            },
            "maxBound": {
              "created": 500,
              "id": "c5",
              "issueID": "i2",
              "text": "e",
            },
          }
        `);
        expect(pushes).toMatchInlineSnapshot(`
          [
            {
              "child": {
                "change": {
                  "oldRow": {
                    "created": 300,
                    "id": "c3",
                    "issueID": "i1",
                    "text": "c",
                  },
                  "row": {
                    "created": 150,
                    "id": "c3",
                    "issueID": "i1",
                    "text": "c2",
                  },
                  "type": "edit",
                },
                "relationshipName": "comments",
              },
              "row": {
                "id": "i1",
              },
              "type": "child",
            },
          ]
        `);
      });

      test('edit row at boundary, making it fall outside the window', () => {
        const {data, messages, storage, pushes} = takeTestWithPartition({
          ...base,
          limit: 2,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
              row: {id: 'c2', issueID: 'i1', created: 350, text: 'b2'},
            },
          ],
        });
        expect(data).toMatchInlineSnapshot(`
          [
            {
              "comments": [
                {
                  "created": 100,
                  "id": "c1",
                  "issueID": "i1",
                  "text": "a",
                  Symbol(rc): 1,
                },
                {
                  "created": 200,
                  "id": "c2",
                  "issueID": "i1",
                  "text": "b",
                  Symbol(rc): 1,
                },
                {
                  "created": 300,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c",
                  Symbol(rc): 1,
                },
              ],
              "id": "i1",
              Symbol(rc): 1,
            },
            {
              "comments": [
                {
                  "created": 400,
                  "id": "c4",
                  "issueID": "i2",
                  "text": "d",
                  Symbol(rc): 1,
                },
                {
                  "created": 500,
                  "id": "c5",
                  "issueID": "i2",
                  "text": "e",
                  Symbol(rc): 1,
                },
              ],
              "id": "i2",
              Symbol(rc): 1,
            },
          ]
        `);
        expect(messages).toMatchInlineSnapshot(`
          [
            [
              ".comments:source(comment)",
              "push",
              {
                "oldRow": {
                  "created": 200,
                  "id": "c2",
                  "issueID": "i1",
                  "text": "b",
                },
                "row": {
                  "created": 350,
                  "id": "c2",
                  "issueID": "i1",
                  "text": "b2",
                },
                "type": "edit",
              },
            ],
            [
              ".comments:source(comment)",
              "fetch",
              {
                "constraint": {
                  "issueID": "i1",
                },
                "start": {
                  "basis": "at",
                  "row": {
                    "created": 200,
                    "id": "c2",
                    "issueID": "i1",
                    "text": "b",
                  },
                },
              },
            ],
            [
              ".comments:take",
              "push",
              {
                "row": {
                  "created": 300,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c",
                },
                "type": "add",
              },
            ],
            [
              ":source(issue)",
              "fetch",
              {
                "constraint": {
                  "id": "i1",
                },
              },
            ],
            [
              ":join(comments)",
              "push",
              {
                "child": {
                  "row": {
                    "created": 300,
                    "id": "c3",
                    "issueID": "i1",
                    "text": "c",
                  },
                  "type": "add",
                },
                "row": {
                  "id": "i1",
                },
                "type": "child",
              },
            ],
          ]
        `);
        expect(storage).toMatchInlineSnapshot(`
          {
            "["take","i1"]": {
              "bound": {
                "created": 300,
                "id": "c3",
                "issueID": "i1",
                "text": "c",
              },
              "size": 2,
            },
            "["take","i2"]": {
              "bound": {
                "created": 500,
                "id": "c5",
                "issueID": "i2",
                "text": "e",
              },
              "size": 2,
            },
            "maxBound": {
              "created": 500,
              "id": "c5",
              "issueID": "i2",
              "text": "e",
            },
          }
        `);
        expect(pushes).toMatchInlineSnapshot(`
          [
            {
              "child": {
                "change": {
                  "node": {
                    "relationships": {},
                    "row": {
                      "created": 300,
                      "id": "c3",
                      "issueID": "i1",
                      "text": "c",
                    },
                  },
                  "type": "add",
                },
                "relationshipName": "comments",
              },
              "row": {
                "id": "i1",
              },
              "type": "child",
            },
          ]
        `);
      });

      test('edit row before boundary, changing its order', () => {
        const {data, messages, storage, pushes} = takeTestWithPartition({
          ...base,
          limit: 3,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
              row: {id: 'c2', issueID: 'i1', created: 50, text: 'b2'},
            },
          ],
        });
        expect(data).toMatchInlineSnapshot(`
          [
            {
              "comments": [
                {
                  "created": 50,
                  "id": "c2",
                  "issueID": "i1",
                  "text": "b2",
                  Symbol(rc): 1,
                },
                {
                  "created": 100,
                  "id": "c1",
                  "issueID": "i1",
                  "text": "a",
                  Symbol(rc): 1,
                },
                {
                  "created": 300,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c",
                  Symbol(rc): 1,
                },
              ],
              "id": "i1",
              Symbol(rc): 1,
            },
            {
              "comments": [
                {
                  "created": 400,
                  "id": "c4",
                  "issueID": "i2",
                  "text": "d",
                  Symbol(rc): 1,
                },
                {
                  "created": 500,
                  "id": "c5",
                  "issueID": "i2",
                  "text": "e",
                  Symbol(rc): 1,
                },
              ],
              "id": "i2",
              Symbol(rc): 1,
            },
          ]
        `);
        expect(messages).toMatchInlineSnapshot(`
          [
            [
              ".comments:source(comment)",
              "push",
              {
                "oldRow": {
                  "created": 200,
                  "id": "c2",
                  "issueID": "i1",
                  "text": "b",
                },
                "row": {
                  "created": 50,
                  "id": "c2",
                  "issueID": "i1",
                  "text": "b2",
                },
                "type": "edit",
              },
            ],
            [
              ".comments:take",
              "push",
              {
                "oldRow": {
                  "created": 200,
                  "id": "c2",
                  "issueID": "i1",
                  "text": "b",
                },
                "row": {
                  "created": 50,
                  "id": "c2",
                  "issueID": "i1",
                  "text": "b2",
                },
                "type": "edit",
              },
            ],
            [
              ":source(issue)",
              "fetch",
              {
                "constraint": {
                  "id": "i1",
                },
              },
            ],
            [
              ":join(comments)",
              "push",
              {
                "child": {
                  "oldRow": {
                    "created": 200,
                    "id": "c2",
                    "issueID": "i1",
                    "text": "b",
                  },
                  "row": {
                    "created": 50,
                    "id": "c2",
                    "issueID": "i1",
                    "text": "b2",
                  },
                  "type": "edit",
                },
                "row": {
                  "id": "i1",
                },
                "type": "child",
              },
            ],
          ]
        `);
        expect(storage).toMatchInlineSnapshot(`
          {
            "["take","i1"]": {
              "bound": {
                "created": 300,
                "id": "c3",
                "issueID": "i1",
                "text": "c",
              },
              "size": 3,
            },
            "["take","i2"]": {
              "bound": {
                "created": 500,
                "id": "c5",
                "issueID": "i2",
                "text": "e",
              },
              "size": 2,
            },
            "maxBound": {
              "created": 500,
              "id": "c5",
              "issueID": "i2",
              "text": "e",
            },
          }
        `);
        expect(pushes).toMatchInlineSnapshot(`
          [
            {
              "child": {
                "change": {
                  "oldRow": {
                    "created": 200,
                    "id": "c2",
                    "issueID": "i1",
                    "text": "b",
                  },
                  "row": {
                    "created": 50,
                    "id": "c2",
                    "issueID": "i1",
                    "text": "b2",
                  },
                  "type": "edit",
                },
                "relationshipName": "comments",
              },
              "row": {
                "id": "i1",
              },
              "type": "child",
            },
          ]
        `);
      });

      test('edit row after boundary to make it the new boundary', () => {
        const {data, messages, storage, pushesWithFetch} =
          takeTestWithPartition({
            ...base,
            limit: 2,
            pushes: [
              {
                type: 'edit',
                oldRow: {id: 'c3', issueID: 'i1', created: 300, text: 'c'},
                row: {id: 'c3', issueID: 'i1', created: 150, text: 'c2'},
              },
            ],
            fetchOnPush: true,
          });
        expect(data).toMatchInlineSnapshot(`
          [
            {
              "comments": [
                {
                  "created": 100,
                  "id": "c1",
                  "issueID": "i1",
                  "text": "a",
                  Symbol(rc): 1,
                },
                {
                  "created": 150,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c2",
                  Symbol(rc): 1,
                },
              ],
              "id": "i1",
              Symbol(rc): 1,
            },
            {
              "comments": [
                {
                  "created": 400,
                  "id": "c4",
                  "issueID": "i2",
                  "text": "d",
                  Symbol(rc): 1,
                },
                {
                  "created": 500,
                  "id": "c5",
                  "issueID": "i2",
                  "text": "e",
                  Symbol(rc): 1,
                },
              ],
              "id": "i2",
              Symbol(rc): 1,
            },
          ]
        `);
        expect(messages).toMatchInlineSnapshot(`
          [
            [
              ".comments:source(comment)",
              "push",
              {
                "oldRow": {
                  "created": 300,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c",
                },
                "row": {
                  "created": 150,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c2",
                },
                "type": "edit",
              },
            ],
            [
              ".comments:source(comment)",
              "fetch",
              {
                "constraint": {
                  "issueID": "i1",
                },
                "reverse": true,
                "start": {
                  "basis": "at",
                  "row": {
                    "created": 200,
                    "id": "c2",
                    "issueID": "i1",
                    "text": "b",
                  },
                },
              },
            ],
            [
              ".comments:take",
              "push",
              {
                "row": {
                  "created": 200,
                  "id": "c2",
                  "issueID": "i1",
                  "text": "b",
                },
                "type": "remove",
              },
            ],
            [
              ":source(issue)",
              "fetch",
              {
                "constraint": {
                  "id": "i1",
                },
              },
            ],
            [
              ":join(comments)",
              "push",
              {
                "child": {
                  "row": {
                    "created": 200,
                    "id": "c2",
                    "issueID": "i1",
                    "text": "b",
                  },
                  "type": "remove",
                },
                "row": {
                  "id": "i1",
                },
                "type": "child",
              },
            ],
            [
              ".comments:take",
              "push",
              {
                "row": {
                  "created": 150,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c2",
                },
                "type": "add",
              },
            ],
            [
              ":source(issue)",
              "fetch",
              {
                "constraint": {
                  "id": "i1",
                },
              },
            ],
            [
              ":join(comments)",
              "push",
              {
                "child": {
                  "row": {
                    "created": 150,
                    "id": "c3",
                    "issueID": "i1",
                    "text": "c2",
                  },
                  "type": "add",
                },
                "row": {
                  "id": "i1",
                },
                "type": "child",
              },
            ],
          ]
        `);
        expect(storage).toMatchInlineSnapshot(`
          {
            "["take","i1"]": {
              "bound": {
                "created": 150,
                "id": "c3",
                "issueID": "i1",
                "text": "c2",
              },
              "size": 2,
            },
            "["take","i2"]": {
              "bound": {
                "created": 500,
                "id": "c5",
                "issueID": "i2",
                "text": "e",
              },
              "size": 2,
            },
            "maxBound": {
              "created": 500,
              "id": "c5",
              "issueID": "i2",
              "text": "e",
            },
          }
        `);
        expect(pushesWithFetch).toMatchInlineSnapshot(`
          [
            {
              "change": {
                "child": {
                  "change": {
                    "node": {
                      "relationships": {},
                      "row": {
                        "created": 200,
                        "id": "c2",
                        "issueID": "i1",
                        "text": "b",
                      },
                    },
                    "type": "remove",
                  },
                  "relationshipName": "comments",
                },
                "row": {
                  "id": "i1",
                },
                "type": "child",
              },
              "fetch": [
                {
                  "relationships": {
                    "comments": [
                      {
                        "relationships": {},
                        "row": {
                          "created": 100,
                          "id": "c1",
                          "issueID": "i1",
                          "text": "a",
                        },
                      },
                    ],
                  },
                  "row": {
                    "id": "i1",
                  },
                },
                {
                  "relationships": {
                    "comments": [
                      {
                        "relationships": {},
                        "row": {
                          "created": 400,
                          "id": "c4",
                          "issueID": "i2",
                          "text": "d",
                        },
                      },
                      {
                        "relationships": {},
                        "row": {
                          "created": 500,
                          "id": "c5",
                          "issueID": "i2",
                          "text": "e",
                        },
                      },
                    ],
                  },
                  "row": {
                    "id": "i2",
                  },
                },
              ],
            },
            {
              "change": {
                "child": {
                  "change": {
                    "node": {
                      "relationships": {},
                      "row": {
                        "created": 150,
                        "id": "c3",
                        "issueID": "i1",
                        "text": "c2",
                      },
                    },
                    "type": "add",
                  },
                  "relationshipName": "comments",
                },
                "row": {
                  "id": "i1",
                },
                "type": "child",
              },
              "fetch": [
                {
                  "relationships": {
                    "comments": [
                      {
                        "relationships": {},
                        "row": {
                          "created": 100,
                          "id": "c1",
                          "issueID": "i1",
                          "text": "a",
                        },
                      },
                      {
                        "relationships": {},
                        "row": {
                          "created": 150,
                          "id": "c3",
                          "issueID": "i1",
                          "text": "c2",
                        },
                      },
                    ],
                  },
                  "row": {
                    "id": "i1",
                  },
                },
                {
                  "relationships": {
                    "comments": [
                      {
                        "relationships": {},
                        "row": {
                          "created": 400,
                          "id": "c4",
                          "issueID": "i2",
                          "text": "d",
                        },
                      },
                      {
                        "relationships": {},
                        "row": {
                          "created": 500,
                          "id": "c5",
                          "issueID": "i2",
                          "text": "e",
                        },
                      },
                    ],
                  },
                  "row": {
                    "id": "i2",
                  },
                },
              ],
            },
          ]
        `);
      });

      test('edit row before boundary to make it new boundary', () => {
        const {data, messages, storage, pushes} = takeTestWithPartition({
          ...base,
          limit: 2,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'c1', issueID: 'i1', created: 100, text: 'a'},
              row: {id: 'c1', issueID: 'i1', created: 250, text: 'a2'},
            },
          ],
        });
        expect(data).toMatchInlineSnapshot(`
          [
            {
              "comments": [
                {
                  "created": 200,
                  "id": "c2",
                  "issueID": "i1",
                  "text": "b",
                  Symbol(rc): 1,
                },
                {
                  "created": 250,
                  "id": "c1",
                  "issueID": "i1",
                  "text": "a2",
                  Symbol(rc): 1,
                },
              ],
              "id": "i1",
              Symbol(rc): 1,
            },
            {
              "comments": [
                {
                  "created": 400,
                  "id": "c4",
                  "issueID": "i2",
                  "text": "d",
                  Symbol(rc): 1,
                },
                {
                  "created": 500,
                  "id": "c5",
                  "issueID": "i2",
                  "text": "e",
                  Symbol(rc): 1,
                },
              ],
              "id": "i2",
              Symbol(rc): 1,
            },
          ]
        `);
        expect(messages).toMatchInlineSnapshot(`
          [
            [
              ".comments:source(comment)",
              "push",
              {
                "oldRow": {
                  "created": 100,
                  "id": "c1",
                  "issueID": "i1",
                  "text": "a",
                },
                "row": {
                  "created": 250,
                  "id": "c1",
                  "issueID": "i1",
                  "text": "a2",
                },
                "type": "edit",
              },
            ],
            [
              ".comments:source(comment)",
              "fetch",
              {
                "constraint": {
                  "issueID": "i1",
                },
                "start": {
                  "basis": "after",
                  "row": {
                    "created": 200,
                    "id": "c2",
                    "issueID": "i1",
                    "text": "b",
                  },
                },
              },
            ],
            [
              ".comments:take",
              "push",
              {
                "oldRow": {
                  "created": 100,
                  "id": "c1",
                  "issueID": "i1",
                  "text": "a",
                },
                "row": {
                  "created": 250,
                  "id": "c1",
                  "issueID": "i1",
                  "text": "a2",
                },
                "type": "edit",
              },
            ],
            [
              ":source(issue)",
              "fetch",
              {
                "constraint": {
                  "id": "i1",
                },
              },
            ],
            [
              ":join(comments)",
              "push",
              {
                "child": {
                  "oldRow": {
                    "created": 100,
                    "id": "c1",
                    "issueID": "i1",
                    "text": "a",
                  },
                  "row": {
                    "created": 250,
                    "id": "c1",
                    "issueID": "i1",
                    "text": "a2",
                  },
                  "type": "edit",
                },
                "row": {
                  "id": "i1",
                },
                "type": "child",
              },
            ],
          ]
        `);
        expect(storage).toMatchInlineSnapshot(`
          {
            "["take","i1"]": {
              "bound": {
                "created": 250,
                "id": "c1",
                "issueID": "i1",
                "text": "a2",
              },
              "size": 2,
            },
            "["take","i2"]": {
              "bound": {
                "created": 500,
                "id": "c5",
                "issueID": "i2",
                "text": "e",
              },
              "size": 2,
            },
            "maxBound": {
              "created": 500,
              "id": "c5",
              "issueID": "i2",
              "text": "e",
            },
          }
        `);
        expect(pushes).toMatchInlineSnapshot(`
          [
            {
              "child": {
                "change": {
                  "oldRow": {
                    "created": 100,
                    "id": "c1",
                    "issueID": "i1",
                    "text": "a",
                  },
                  "row": {
                    "created": 250,
                    "id": "c1",
                    "issueID": "i1",
                    "text": "a2",
                  },
                  "type": "edit",
                },
                "relationshipName": "comments",
              },
              "row": {
                "id": "i1",
              },
              "type": "child",
            },
          ]
        `);
      });

      test('edit row before boundary to fetch new boundary', () => {
        const {data, messages, storage, pushesWithFetch} =
          takeTestWithPartition({
            ...base,
            limit: 2,
            pushes: [
              {
                type: 'edit',
                oldRow: {id: 'c1', issueID: 'i1', created: 100, text: 'a'},
                row: {id: 'c1', issueID: 'i1', created: 350, text: 'a2'},
              },
            ],
            fetchOnPush: true,
          });
        expect(data).toMatchInlineSnapshot(`
          [
            {
              "comments": [
                {
                  "created": 200,
                  "id": "c2",
                  "issueID": "i1",
                  "text": "b",
                  Symbol(rc): 1,
                },
                {
                  "created": 300,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c",
                  Symbol(rc): 1,
                },
              ],
              "id": "i1",
              Symbol(rc): 1,
            },
            {
              "comments": [
                {
                  "created": 400,
                  "id": "c4",
                  "issueID": "i2",
                  "text": "d",
                  Symbol(rc): 1,
                },
                {
                  "created": 500,
                  "id": "c5",
                  "issueID": "i2",
                  "text": "e",
                  Symbol(rc): 1,
                },
              ],
              "id": "i2",
              Symbol(rc): 1,
            },
          ]
        `);
        expect(messages).toMatchInlineSnapshot(`
          [
            [
              ".comments:source(comment)",
              "push",
              {
                "oldRow": {
                  "created": 100,
                  "id": "c1",
                  "issueID": "i1",
                  "text": "a",
                },
                "row": {
                  "created": 350,
                  "id": "c1",
                  "issueID": "i1",
                  "text": "a2",
                },
                "type": "edit",
              },
            ],
            [
              ".comments:source(comment)",
              "fetch",
              {
                "constraint": {
                  "issueID": "i1",
                },
                "start": {
                  "basis": "after",
                  "row": {
                    "created": 200,
                    "id": "c2",
                    "issueID": "i1",
                    "text": "b",
                  },
                },
              },
            ],
            [
              ".comments:take",
              "push",
              {
                "row": {
                  "created": 100,
                  "id": "c1",
                  "issueID": "i1",
                  "text": "a",
                },
                "type": "remove",
              },
            ],
            [
              ":source(issue)",
              "fetch",
              {
                "constraint": {
                  "id": "i1",
                },
              },
            ],
            [
              ":join(comments)",
              "push",
              {
                "child": {
                  "row": {
                    "created": 100,
                    "id": "c1",
                    "issueID": "i1",
                    "text": "a",
                  },
                  "type": "remove",
                },
                "row": {
                  "id": "i1",
                },
                "type": "child",
              },
            ],
            [
              ".comments:take",
              "push",
              {
                "row": {
                  "created": 300,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c",
                },
                "type": "add",
              },
            ],
            [
              ":source(issue)",
              "fetch",
              {
                "constraint": {
                  "id": "i1",
                },
              },
            ],
            [
              ":join(comments)",
              "push",
              {
                "child": {
                  "row": {
                    "created": 300,
                    "id": "c3",
                    "issueID": "i1",
                    "text": "c",
                  },
                  "type": "add",
                },
                "row": {
                  "id": "i1",
                },
                "type": "child",
              },
            ],
          ]
        `);
        expect(storage).toMatchInlineSnapshot(`
          {
            "["take","i1"]": {
              "bound": {
                "created": 300,
                "id": "c3",
                "issueID": "i1",
                "text": "c",
              },
              "size": 2,
            },
            "["take","i2"]": {
              "bound": {
                "created": 500,
                "id": "c5",
                "issueID": "i2",
                "text": "e",
              },
              "size": 2,
            },
            "maxBound": {
              "created": 500,
              "id": "c5",
              "issueID": "i2",
              "text": "e",
            },
          }
        `);
        expect(pushesWithFetch).toMatchInlineSnapshot(`
          [
            {
              "change": {
                "child": {
                  "change": {
                    "node": {
                      "relationships": {},
                      "row": {
                        "created": 100,
                        "id": "c1",
                        "issueID": "i1",
                        "text": "a",
                      },
                    },
                    "type": "remove",
                  },
                  "relationshipName": "comments",
                },
                "row": {
                  "id": "i1",
                },
                "type": "child",
              },
              "fetch": [
                {
                  "relationships": {
                    "comments": [
                      {
                        "relationships": {},
                        "row": {
                          "created": 200,
                          "id": "c2",
                          "issueID": "i1",
                          "text": "b",
                        },
                      },
                    ],
                  },
                  "row": {
                    "id": "i1",
                  },
                },
                {
                  "relationships": {
                    "comments": [
                      {
                        "relationships": {},
                        "row": {
                          "created": 400,
                          "id": "c4",
                          "issueID": "i2",
                          "text": "d",
                        },
                      },
                      {
                        "relationships": {},
                        "row": {
                          "created": 500,
                          "id": "c5",
                          "issueID": "i2",
                          "text": "e",
                        },
                      },
                    ],
                  },
                  "row": {
                    "id": "i2",
                  },
                },
              ],
            },
            {
              "change": {
                "child": {
                  "change": {
                    "node": {
                      "relationships": {},
                      "row": {
                        "created": 300,
                        "id": "c3",
                        "issueID": "i1",
                        "text": "c",
                      },
                    },
                    "type": "add",
                  },
                  "relationshipName": "comments",
                },
                "row": {
                  "id": "i1",
                },
                "type": "child",
              },
              "fetch": [
                {
                  "relationships": {
                    "comments": [
                      {
                        "relationships": {},
                        "row": {
                          "created": 200,
                          "id": "c2",
                          "issueID": "i1",
                          "text": "b",
                        },
                      },
                      {
                        "relationships": {},
                        "row": {
                          "created": 300,
                          "id": "c3",
                          "issueID": "i1",
                          "text": "c",
                        },
                      },
                    ],
                  },
                  "row": {
                    "id": "i1",
                  },
                },
                {
                  "relationships": {
                    "comments": [
                      {
                        "relationships": {},
                        "row": {
                          "created": 400,
                          "id": "c4",
                          "issueID": "i2",
                          "text": "d",
                        },
                      },
                      {
                        "relationships": {},
                        "row": {
                          "created": 500,
                          "id": "c5",
                          "issueID": "i2",
                          "text": "e",
                        },
                      },
                    ],
                  },
                  "row": {
                    "id": "i2",
                  },
                },
              ],
            },
          ]
        `);
      });
    });

    describe('changing partition value', () => {
      test('move to from first partition to second', () => {
        const {data, messages, storage, pushesWithFetch} =
          takeTestWithPartition({
            ...base,
            limit: 2,
            pushes: [
              {
                type: 'edit',
                oldRow: {id: 'c1', issueID: 'i1', created: 100, text: 'a'},
                row: {id: 'c1', issueID: 'i2', created: 100, text: 'a2'},
              },
            ],
            fetchOnPush: true,
          });
        expect(data).toMatchInlineSnapshot(`
          [
            {
              "comments": [
                {
                  "created": 200,
                  "id": "c2",
                  "issueID": "i1",
                  "text": "b",
                  Symbol(rc): 1,
                },
                {
                  "created": 300,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c",
                  Symbol(rc): 1,
                },
              ],
              "id": "i1",
              Symbol(rc): 1,
            },
            {
              "comments": [
                {
                  "created": 100,
                  "id": "c1",
                  "issueID": "i2",
                  "text": "a2",
                  Symbol(rc): 1,
                },
                {
                  "created": 400,
                  "id": "c4",
                  "issueID": "i2",
                  "text": "d",
                  Symbol(rc): 1,
                },
              ],
              "id": "i2",
              Symbol(rc): 1,
            },
          ]
        `);
        expect(messages).toMatchInlineSnapshot(`
          [
            [
              ".comments:source(comment)",
              "push",
              {
                "row": {
                  "created": 100,
                  "id": "c1",
                  "issueID": "i1",
                  "text": "a",
                },
                "type": "remove",
              },
            ],
            [
              ".comments:source(comment)",
              "fetch",
              {
                "constraint": {
                  "issueID": "i1",
                },
                "reverse": true,
                "start": {
                  "basis": "after",
                  "row": {
                    "created": 200,
                    "id": "c2",
                    "issueID": "i1",
                    "text": "b",
                  },
                },
              },
            ],
            [
              ".comments:source(comment)",
              "fetch",
              {
                "constraint": {
                  "issueID": "i1",
                },
                "start": {
                  "basis": "at",
                  "row": {
                    "created": 200,
                    "id": "c2",
                    "issueID": "i1",
                    "text": "b",
                  },
                },
              },
            ],
            [
              ".comments:take",
              "push",
              {
                "row": {
                  "created": 100,
                  "id": "c1",
                  "issueID": "i1",
                  "text": "a",
                },
                "type": "remove",
              },
            ],
            [
              ":source(issue)",
              "fetch",
              {
                "constraint": {
                  "id": "i1",
                },
              },
            ],
            [
              ":join(comments)",
              "push",
              {
                "child": {
                  "row": {
                    "created": 100,
                    "id": "c1",
                    "issueID": "i1",
                    "text": "a",
                  },
                  "type": "remove",
                },
                "row": {
                  "id": "i1",
                },
                "type": "child",
              },
            ],
            [
              ".comments:take",
              "push",
              {
                "row": {
                  "created": 300,
                  "id": "c3",
                  "issueID": "i1",
                  "text": "c",
                },
                "type": "add",
              },
            ],
            [
              ":source(issue)",
              "fetch",
              {
                "constraint": {
                  "id": "i1",
                },
              },
            ],
            [
              ":join(comments)",
              "push",
              {
                "child": {
                  "row": {
                    "created": 300,
                    "id": "c3",
                    "issueID": "i1",
                    "text": "c",
                  },
                  "type": "add",
                },
                "row": {
                  "id": "i1",
                },
                "type": "child",
              },
            ],
            [
              ".comments:source(comment)",
              "push",
              {
                "row": {
                  "created": 100,
                  "id": "c1",
                  "issueID": "i2",
                  "text": "a2",
                },
                "type": "add",
              },
            ],
            [
              ".comments:source(comment)",
              "fetch",
              {
                "constraint": {
                  "issueID": "i2",
                },
                "reverse": true,
                "start": {
                  "basis": "at",
                  "row": {
                    "created": 500,
                    "id": "c5",
                    "issueID": "i2",
                    "text": "e",
                  },
                },
              },
            ],
            [
              ".comments:take",
              "push",
              {
                "row": {
                  "created": 500,
                  "id": "c5",
                  "issueID": "i2",
                  "text": "e",
                },
                "type": "remove",
              },
            ],
            [
              ":source(issue)",
              "fetch",
              {
                "constraint": {
                  "id": "i2",
                },
              },
            ],
            [
              ":join(comments)",
              "push",
              {
                "child": {
                  "row": {
                    "created": 500,
                    "id": "c5",
                    "issueID": "i2",
                    "text": "e",
                  },
                  "type": "remove",
                },
                "row": {
                  "id": "i2",
                },
                "type": "child",
              },
            ],
            [
              ".comments:take",
              "push",
              {
                "row": {
                  "created": 100,
                  "id": "c1",
                  "issueID": "i2",
                  "text": "a2",
                },
                "type": "add",
              },
            ],
            [
              ":source(issue)",
              "fetch",
              {
                "constraint": {
                  "id": "i2",
                },
              },
            ],
            [
              ":join(comments)",
              "push",
              {
                "child": {
                  "row": {
                    "created": 100,
                    "id": "c1",
                    "issueID": "i2",
                    "text": "a2",
                  },
                  "type": "add",
                },
                "row": {
                  "id": "i2",
                },
                "type": "child",
              },
            ],
          ]
        `);
        expect(storage).toMatchInlineSnapshot(`
          {
            "["take","i1"]": {
              "bound": {
                "created": 300,
                "id": "c3",
                "issueID": "i1",
                "text": "c",
              },
              "size": 2,
            },
            "["take","i2"]": {
              "bound": {
                "created": 400,
                "id": "c4",
                "issueID": "i2",
                "text": "d",
              },
              "size": 2,
            },
            "maxBound": {
              "created": 500,
              "id": "c5",
              "issueID": "i2",
              "text": "e",
            },
          }
        `);
        expect(pushesWithFetch).toMatchInlineSnapshot(`
          [
            {
              "change": {
                "child": {
                  "change": {
                    "node": {
                      "relationships": {},
                      "row": {
                        "created": 100,
                        "id": "c1",
                        "issueID": "i1",
                        "text": "a",
                      },
                    },
                    "type": "remove",
                  },
                  "relationshipName": "comments",
                },
                "row": {
                  "id": "i1",
                },
                "type": "child",
              },
              "fetch": [
                {
                  "relationships": {
                    "comments": [
                      {
                        "relationships": {},
                        "row": {
                          "created": 200,
                          "id": "c2",
                          "issueID": "i1",
                          "text": "b",
                        },
                      },
                    ],
                  },
                  "row": {
                    "id": "i1",
                  },
                },
                {
                  "relationships": {
                    "comments": [
                      {
                        "relationships": {},
                        "row": {
                          "created": 400,
                          "id": "c4",
                          "issueID": "i2",
                          "text": "d",
                        },
                      },
                      {
                        "relationships": {},
                        "row": {
                          "created": 500,
                          "id": "c5",
                          "issueID": "i2",
                          "text": "e",
                        },
                      },
                    ],
                  },
                  "row": {
                    "id": "i2",
                  },
                },
              ],
            },
            {
              "change": {
                "child": {
                  "change": {
                    "node": {
                      "relationships": {},
                      "row": {
                        "created": 300,
                        "id": "c3",
                        "issueID": "i1",
                        "text": "c",
                      },
                    },
                    "type": "add",
                  },
                  "relationshipName": "comments",
                },
                "row": {
                  "id": "i1",
                },
                "type": "child",
              },
              "fetch": [
                {
                  "relationships": {
                    "comments": [
                      {
                        "relationships": {},
                        "row": {
                          "created": 200,
                          "id": "c2",
                          "issueID": "i1",
                          "text": "b",
                        },
                      },
                      {
                        "relationships": {},
                        "row": {
                          "created": 300,
                          "id": "c3",
                          "issueID": "i1",
                          "text": "c",
                        },
                      },
                    ],
                  },
                  "row": {
                    "id": "i1",
                  },
                },
                {
                  "relationships": {
                    "comments": [
                      {
                        "relationships": {},
                        "row": {
                          "created": 400,
                          "id": "c4",
                          "issueID": "i2",
                          "text": "d",
                        },
                      },
                      {
                        "relationships": {},
                        "row": {
                          "created": 500,
                          "id": "c5",
                          "issueID": "i2",
                          "text": "e",
                        },
                      },
                    ],
                  },
                  "row": {
                    "id": "i2",
                  },
                },
              ],
            },
            {
              "change": {
                "child": {
                  "change": {
                    "node": {
                      "relationships": {},
                      "row": {
                        "created": 500,
                        "id": "c5",
                        "issueID": "i2",
                        "text": "e",
                      },
                    },
                    "type": "remove",
                  },
                  "relationshipName": "comments",
                },
                "row": {
                  "id": "i2",
                },
                "type": "child",
              },
              "fetch": [
                {
                  "relationships": {
                    "comments": [
                      {
                        "relationships": {},
                        "row": {
                          "created": 200,
                          "id": "c2",
                          "issueID": "i1",
                          "text": "b",
                        },
                      },
                      {
                        "relationships": {},
                        "row": {
                          "created": 300,
                          "id": "c3",
                          "issueID": "i1",
                          "text": "c",
                        },
                      },
                    ],
                  },
                  "row": {
                    "id": "i1",
                  },
                },
                {
                  "relationships": {
                    "comments": [
                      {
                        "relationships": {},
                        "row": {
                          "created": 400,
                          "id": "c4",
                          "issueID": "i2",
                          "text": "d",
                        },
                      },
                    ],
                  },
                  "row": {
                    "id": "i2",
                  },
                },
              ],
            },
            {
              "change": {
                "child": {
                  "change": {
                    "node": {
                      "relationships": {},
                      "row": {
                        "created": 100,
                        "id": "c1",
                        "issueID": "i2",
                        "text": "a2",
                      },
                    },
                    "type": "add",
                  },
                  "relationshipName": "comments",
                },
                "row": {
                  "id": "i2",
                },
                "type": "child",
              },
              "fetch": [
                {
                  "relationships": {
                    "comments": [
                      {
                        "relationships": {},
                        "row": {
                          "created": 200,
                          "id": "c2",
                          "issueID": "i1",
                          "text": "b",
                        },
                      },
                      {
                        "relationships": {},
                        "row": {
                          "created": 300,
                          "id": "c3",
                          "issueID": "i1",
                          "text": "c",
                        },
                      },
                    ],
                  },
                  "row": {
                    "id": "i1",
                  },
                },
                {
                  "relationships": {
                    "comments": [
                      {
                        "relationships": {},
                        "row": {
                          "created": 100,
                          "id": "c1",
                          "issueID": "i2",
                          "text": "a2",
                        },
                      },
                      {
                        "relationships": {},
                        "row": {
                          "created": 400,
                          "id": "c4",
                          "issueID": "i2",
                          "text": "d",
                        },
                      },
                    ],
                  },
                  "row": {
                    "id": "i2",
                  },
                },
              ],
            },
          ]
        `);
      });
    });
  });
});

type TakeTest = {
  sourceRows: readonly Row[];
  limit: number;
  pushes: SourceChange[];
  fetchOnPush?: boolean | undefined;
};

function takeNoPartitionTest(t: TakeTest) {
  const testTableName = 'testTable';
  const ast: AST = {
    table: 'testTable',
    orderBy: [
      ['created', 'asc'],
      ['id', 'asc'],
    ],
    limit: t.limit,
  } as const;
  const result = runPushTest({
    sources: {
      [testTableName]: {
        primaryKeys: ['id'],
        columns: {
          id: {type: 'string'},
          created: {type: 'number'},
          text: {type: 'string', nullable: true},
        },
      },
    },
    sourceContents: {
      [testTableName]: t.sourceRows,
    },
    ast,
    format: {
      singular: t.limit === 1,
      relationships: {},
    },
    pushes: t.pushes.map(change => [testTableName, change]),
    fetchOnPush: t.fetchOnPush,
  });
  if (result.pushes.length > 1 && !t.fetchOnPush) {
    throw new Error('Test with multiple pushes should use fetchOnPush');
  }
  return {
    data: result.data,
    messages: result.log,
    storage: result.actualStorage[':take'],
    pushes: result.pushes,
    pushesWithFetch: result.pushesWithFetch,
  };
}

function takeTestWithPartition(t: TakeTest) {
  const sources: Sources = {
    issue: {
      columns: {
        id: {type: 'string'},
      },
      primaryKeys: ['id'],
    },
    comment: {
      columns: {
        id: {type: 'string'},
        issueID: {type: 'string'},
        created: {type: 'number'},
        text: {type: 'string', nullable: true},
      },
      primaryKeys: ['id'],
    },
  } as const;

  const ast: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
    related: [
      {
        system: 'client',
        correlation: {parentField: ['id'], childField: ['issueID']},
        subquery: {
          table: 'comment',
          alias: 'comments',
          orderBy: [
            ['created', 'asc'],
            ['id', 'asc'],
          ],
          limit: t.limit,
        },
      },
    ],
  } as const;

  const result = runPushTest({
    sources,
    sourceContents: {
      issue: [{id: 'i1'}, {id: 'i2'}],
      comment: t.sourceRows,
    },
    ast,
    format: {
      singular: false,
      relationships: {
        comments: {
          singular: t.limit === 1,
          relationships: {},
        },
      },
    },
    pushes: t.pushes.map(change => ['comment', change]),
    fetchOnPush: t.fetchOnPush,
  });
  if (result.pushes.length > 1 && !t.fetchOnPush) {
    throw new Error('Test with multiple pushes should use fetchOnPush');
  }
  return {
    data: result.data,
    messages: result.log,
    storage: result.actualStorage['.comments:take'],
    pushes: result.pushes,
    pushesWithFetch: result.pushesWithFetch,
  };
}
