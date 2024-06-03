import {describe, expect, test} from 'vitest';
import type {ServerAST} from '../../zql/server-ast.js';
import {minifyAliases} from './queries.js';

describe('view-syncer/queries/minify-aliases', () => {
  test('no sub queries', () => {
    const query: ServerAST = {
      select: [
        [['public.issueLabel', '_0_version'], 'public/issueLabel/_0_version'],
        [['public.issueLabel', 'id'], 'public/issueLabel/id'],
        [['public.issueLabel', 'issueID'], 'public/issueLabel/issueID'],
        [['public.issueLabel', 'labelID'], 'public/issueLabel/labelID'],
      ],
      table: 'issueLabel',
    };

    const {ast, columnAliases} = minifyAliases(query);
    expect(ast).toEqual({
      select: [
        [['public.issueLabel', '_0_version'], 'a'],
        [['public.issueLabel', 'id'], 'b'],
        [['public.issueLabel', 'issueID'], 'c'],
        [['public.issueLabel', 'labelID'], 'd'],
      ],
      table: 'issueLabel',
    });

    expect(columnAliases).toMatchInlineSnapshot(`
      Map {
        "a" => {
          "column": "_0_version",
          "rowAlias": "public/issueLabel",
          "schema": "public",
          "subQueryName": "",
          "table": "issueLabel",
        },
        "b" => {
          "column": "id",
          "rowAlias": "public/issueLabel",
          "schema": "public",
          "subQueryName": "",
          "table": "issueLabel",
        },
        "c" => {
          "column": "issueID",
          "rowAlias": "public/issueLabel",
          "schema": "public",
          "subQueryName": "",
          "table": "issueLabel",
        },
        "d" => {
          "column": "labelID",
          "rowAlias": "public/issueLabel",
          "schema": "public",
          "subQueryName": "",
          "table": "issueLabel",
        },
      }
    `);
  });

  test('sub queries', () => {
    const query: ServerAST = {
      select: [
        [['issue', 'public/issue/_0_version'], 'issue/public/issue/_0_version'],
        [['issue', 'public/issue/created'], 'issue/public/issue/created'],
        [['issue', 'public/issue/creatorID'], 'issue/public/issue/creatorID'],
        [
          ['issue', 'public/issue/description'],
          'issue/public/issue/description',
        ],
        [['issue', 'public/issue/id'], 'issue/public/issue/id'],
        [
          ['issue', 'public/issue/kanbanOrder'],
          'issue/public/issue/kanbanOrder',
        ],
        [['issue', 'public/issue/modified'], 'issue/public/issue/modified'],
        [['issue', 'public/issue/priority'], 'issue/public/issue/priority'],
        [['issue', 'public/issue/status'], 'issue/public/issue/status'],
        [['issue', 'public/issue/title'], 'issue/public/issue/title'],
        [
          ['issueLabel', 'public/issueLabel/_0_version'],
          'issueLabel/public/issueLabel/_0_version',
        ],
        [
          ['issueLabel', 'public/issueLabel/id'],
          'issueLabel/public/issueLabel/id',
        ],
        [
          ['issueLabel', 'public/issueLabel/issueID'],
          'issueLabel/public/issueLabel/issueID',
        ],
        [
          ['issueLabel', 'public/issueLabel/labelID'],
          'issueLabel/public/issueLabel/labelID',
        ],
        [['label', 'public/label/_0_version'], 'label/public/label/_0_version'],
        [['label', 'public/label/id'], 'label/public/label/id'],
        [['label', 'public/label/name'], 'label/public/label/name'],
      ],
      table: 'issues',
    };

    const {ast, columnAliases} = minifyAliases(query);

    expect(ast).toEqual({
      select: [
        [['issue', 'public/issue/_0_version'], 'a'],
        [['issue', 'public/issue/created'], 'b'],
        [['issue', 'public/issue/creatorID'], 'c'],
        [['issue', 'public/issue/description'], 'd'],
        [['issue', 'public/issue/id'], 'e'],
        [['issue', 'public/issue/kanbanOrder'], 'f'],
        [['issue', 'public/issue/modified'], 'g'],
        [['issue', 'public/issue/priority'], 'h'],
        [['issue', 'public/issue/status'], 'i'],
        [['issue', 'public/issue/title'], 'j'],
        [['issueLabel', 'public/issueLabel/_0_version'], 'k'],
        [['issueLabel', 'public/issueLabel/id'], 'l'],
        [['issueLabel', 'public/issueLabel/issueID'], 'm'],
        [['issueLabel', 'public/issueLabel/labelID'], 'n'],
        [['label', 'public/label/_0_version'], 'o'],
        [['label', 'public/label/id'], 'p'],
        [['label', 'public/label/name'], 'q'],
      ],
      table: 'issues',
    });

    expect(columnAliases).toMatchInlineSnapshot(`
      Map {
        "a" => {
          "column": "_0_version",
          "rowAlias": "issue/public/issue",
          "schema": "public",
          "subQueryName": "issue",
          "table": "issue",
        },
        "b" => {
          "column": "created",
          "rowAlias": "issue/public/issue",
          "schema": "public",
          "subQueryName": "issue",
          "table": "issue",
        },
        "c" => {
          "column": "creatorID",
          "rowAlias": "issue/public/issue",
          "schema": "public",
          "subQueryName": "issue",
          "table": "issue",
        },
        "d" => {
          "column": "description",
          "rowAlias": "issue/public/issue",
          "schema": "public",
          "subQueryName": "issue",
          "table": "issue",
        },
        "e" => {
          "column": "id",
          "rowAlias": "issue/public/issue",
          "schema": "public",
          "subQueryName": "issue",
          "table": "issue",
        },
        "f" => {
          "column": "kanbanOrder",
          "rowAlias": "issue/public/issue",
          "schema": "public",
          "subQueryName": "issue",
          "table": "issue",
        },
        "g" => {
          "column": "modified",
          "rowAlias": "issue/public/issue",
          "schema": "public",
          "subQueryName": "issue",
          "table": "issue",
        },
        "h" => {
          "column": "priority",
          "rowAlias": "issue/public/issue",
          "schema": "public",
          "subQueryName": "issue",
          "table": "issue",
        },
        "i" => {
          "column": "status",
          "rowAlias": "issue/public/issue",
          "schema": "public",
          "subQueryName": "issue",
          "table": "issue",
        },
        "j" => {
          "column": "title",
          "rowAlias": "issue/public/issue",
          "schema": "public",
          "subQueryName": "issue",
          "table": "issue",
        },
        "k" => {
          "column": "_0_version",
          "rowAlias": "issueLabel/public/issueLabel",
          "schema": "public",
          "subQueryName": "issueLabel",
          "table": "issueLabel",
        },
        "l" => {
          "column": "id",
          "rowAlias": "issueLabel/public/issueLabel",
          "schema": "public",
          "subQueryName": "issueLabel",
          "table": "issueLabel",
        },
        "m" => {
          "column": "issueID",
          "rowAlias": "issueLabel/public/issueLabel",
          "schema": "public",
          "subQueryName": "issueLabel",
          "table": "issueLabel",
        },
        "n" => {
          "column": "labelID",
          "rowAlias": "issueLabel/public/issueLabel",
          "schema": "public",
          "subQueryName": "issueLabel",
          "table": "issueLabel",
        },
        "o" => {
          "column": "_0_version",
          "rowAlias": "label/public/label",
          "schema": "public",
          "subQueryName": "label",
          "table": "label",
        },
        "p" => {
          "column": "id",
          "rowAlias": "label/public/label",
          "schema": "public",
          "subQueryName": "label",
          "table": "label",
        },
        "q" => {
          "column": "name",
          "rowAlias": "label/public/label",
          "schema": "public",
          "subQueryName": "label",
          "table": "label",
        },
      }
    `);
  });

  test('alias name generation (lots of columns)', () => {
    const ast: ServerAST = {
      table: 'foo',
      select: Array.from({length: 100}, (_, i) => [
        ['table', `col_${i}`],
        `foo/bar/baz/col_${i}`,
      ]),
    };
    const {columnAliases} = minifyAliases(ast);
    expect([...columnAliases.keys()]).toMatchInlineSnapshot(`
      [
        "a",
        "b",
        "c",
        "d",
        "e",
        "f",
        "g",
        "h",
        "i",
        "j",
        "k",
        "l",
        "m",
        "n",
        "o",
        "p",
        "q",
        "r",
        "s",
        "t",
        "u",
        "v",
        "w",
        "x",
        "y",
        "z",
        "A",
        "B",
        "C",
        "D",
        "E",
        "F",
        "G",
        "H",
        "I",
        "J",
        "K",
        "L",
        "M",
        "N",
        "O",
        "P",
        "Q",
        "R",
        "S",
        "T",
        "U",
        "V",
        "W",
        "X",
        "Y",
        "Z",
        "a1",
        "b1",
        "c1",
        "d1",
        "e1",
        "f1",
        "g1",
        "h1",
        "i1",
        "j1",
        "k1",
        "l1",
        "m1",
        "n1",
        "o1",
        "p1",
        "q1",
        "r1",
        "s1",
        "t1",
        "u1",
        "v1",
        "w1",
        "x1",
        "y1",
        "z1",
        "A1",
        "B1",
        "C1",
        "D1",
        "E1",
        "F1",
        "G1",
        "H1",
        "I1",
        "J1",
        "K1",
        "L1",
        "M1",
        "N1",
        "O1",
        "P1",
        "Q1",
        "R1",
        "S1",
        "T1",
        "U1",
        "V1",
      ]
    `);
  });
});
