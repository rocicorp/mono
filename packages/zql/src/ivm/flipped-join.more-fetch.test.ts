import {expect, suite, test} from 'vitest';
import {runFetchTest, type Sources} from './test/fetch-and-push-tests.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {Format} from './view.ts';

suite('one:many:one', () => {
  const sources: Sources = {
    issue: {
      columns: {
        id: {type: 'string'},
      },
      primaryKeys: ['id'],
    },
    issueLabel: {
      columns: {
        issueID: {type: 'string'},
        labelID: {type: 'string'},
      },
      primaryKeys: ['issueID', 'labelID'],
    },
    label: {
      columns: {
        id: {type: 'string'},
        name: {type: 'string'},
      },
      primaryKeys: ['id'],
    },
  } as const;

  const ast: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
    where: {
      type: 'and',
      conditions: [
        {
          type: 'correlatedSubquery',
          related: {
            system: 'client',
            correlation: {parentField: ['id'], childField: ['issueID']},
            subquery: {
              table: 'issueLabel',
              alias: 'issueLabels_1',
              orderBy: [
                ['issueID', 'asc'],
                ['labelID', 'asc'],
              ],
              where: {
                type: 'correlatedSubquery',
                related: {
                  system: 'client',
                  correlation: {
                    parentField: ['labelID'],
                    childField: ['id'],
                  },
                  subquery: {
                    table: 'label',
                    alias: 'labels_1',
                    where: {
                      type: 'simple',
                      op: '=',
                      left: {type: 'column', name: 'name'},
                      right: {type: 'literal', value: 'label1'},
                    },
                    orderBy: [['id', 'asc']],
                  },
                },
                op: 'EXISTS',
                flip: true,
              },
            },
          },
          op: 'EXISTS',
          flip: true,
        },
        {
          type: 'correlatedSubquery',
          related: {
            system: 'client',
            correlation: {parentField: ['id'], childField: ['issueID']},
            subquery: {
              table: 'issueLabel',
              alias: 'issueLabels_2',
              orderBy: [
                ['issueID', 'asc'],
                ['labelID', 'asc'],
              ],
              where: {
                type: 'correlatedSubquery',
                related: {
                  system: 'client',
                  correlation: {
                    parentField: ['labelID'],
                    childField: ['id'],
                  },
                  subquery: {
                    table: 'label',
                    alias: 'labels_2',
                    where: {
                      type: 'simple',
                      op: '=',
                      left: {type: 'column', name: 'name'},
                      right: {type: 'literal', value: 'label2'},
                    },
                    orderBy: [['id', 'asc']],
                  },
                },
                op: 'EXISTS',
                flip: true,
              },
            },
          },
          op: 'EXISTS',
          flip: true,
        },
      ],
    },
  };

  const format: Format = {
    singular: false,
    relationships: {
      issueLabels_1: {
        singular: false,
        relationships: {
          labels: {
            singular: true,
            relationships: {},
          },
        },
      },
      issueLabels_2: {
        singular: false,
        relationships: {
          labels: {
            singular: true,
            relationships: {},
          },
        },
      },
    },
  } as const;

  test('when flipped-joins are chained parent constraints are translated to child constraints', () => {
    const {log, actualStorage, data} = runFetchTest({
      sources,
      ast,
      format,
      sourceContents: {
        issue: [{id: 'i1'}, {id: 'i2'}],
        issueLabel: [
          {issueID: 'i1', labelID: 'l1'},
          {issueID: 'i1', labelID: 'l2'},
          {issueID: 'i2', labelID: 'l1'},
        ],
        label: [
          {id: 'l1', name: 'label1'},
          {id: 'l2', name: 'label2'},
        ],
      },
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "id": "i1",
          Symbol(rc): 1,
        },
      ]
    `);

    expect(actualStorage).toMatchInlineSnapshot(`{}`);

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ":flipped-join(issueLabels_2_1)",
          "fetch",
          {},
        ],
        [
          ".issueLabels_2_1:flipped-join(labels_2)",
          "fetch",
          {},
        ],
        [
          ".issueLabels_2_1.labels_2:source(label)",
          "fetch",
          {},
        ],
        [
          ".issueLabels_2_1:source(issueLabel)",
          "fetch",
          {
            "constraint": {
              "labelID": "l2",
            },
          },
        ],
        [
          ":flipped-join(issueLabels_1_0)",
          "fetch",
          {
            "constraint": {
              "id": "i1",
            },
          },
        ],
        [
          ".issueLabels_1_0:flipped-join(labels_1)",
          "fetch",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
        [
          ".issueLabels_1_0.labels_1:source(label)",
          "fetch",
          {},
        ],
        [
          ".issueLabels_1_0:source(issueLabel)",
          "fetch",
          {
            "constraint": {
              "issueID": "i1",
              "labelID": "l1",
            },
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
      ]
    `);
  });
});
