import {assert} from 'shared/src/asserts.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import type {AST} from 'zql/src/zql/ast/ast.js';
import {testDBs} from '../../test/db.js';
import type {PostgresDB} from '../../types/pg.js';
import {Normalized} from '../../zql/normalize.js';
import {getPublicationInfo} from '../replicator/tables/published.js';
import {QueryHandler} from './queries.js';

describe('view-syncer/queries', () => {
  let db: PostgresDB;

  beforeEach(async () => {
    db = await testDBs.create('view_syncer_queries_test');
    await db`
    CREATE TABLE issues (
      id text PRIMARY KEY,
      owner_id text,
      parent_id text,
      title text,
      _0_version VARCHAR(38)
    );
    CREATE TABLE users (
      id text PRIMARY KEY,
      name text,
      _0_version VARCHAR(38)
    );
    CREATE TABLE "issueLabels" (
      id text PRIMARY KEY,
      "issueId" text,
      "labelId" text,
      _0_version VARCHAR(38)
    );
    CREATE TABLE labels (
      id text PRIMARY KEY,
      name text,
      _0_version VARCHAR(38)
    );

    INSERT INTO users (id, name, _0_version) VALUES (100, 'Alice', '0a');
    INSERT INTO users (id, name, _0_version) VALUES (101, 'Bob', '0b');
    INSERT INTO users (id, name, _0_version) VALUES (102, 'Candice', '0c');

    INSERT INTO issues (id, title, owner_id, _0_version) VALUES (1, 'parent issue foo', 100, '1a0');
    INSERT INTO issues (id, title, owner_id, _0_version) VALUES (2, 'parent issue bar', 101, '1ab');
    INSERT INTO issues (id, title, owner_id, parent_id, _0_version) VALUES (3, 'foo', 102, 1, '1ca');
    INSERT INTO issues (id, title, owner_id, parent_id, _0_version) VALUES (4, 'bar', 101, 2, '1cd');

    INSERT INTO labels (id, name, _0_version) VALUES (1, 'bug', '1a');
    INSERT INTO labels (id, name, _0_version) VALUES (2, 'feature', '1b');
    INSERT INTO labels (id, name, _0_version) VALUES (3, 'enhancement', '1c');

    INSERT INTO "issueLabels" (id, "issueId", "labelId", _0_version) VALUES (1, 3, 1, '1a');
    INSERT INTO "issueLabels" (id, "issueId", "labelId", _0_version) VALUES (2, 3, 2, '1b');
    INSERT INTO "issueLabels" (id, "issueId", "labelId", _0_version) VALUES (3, 4, 2, '1c');

    CREATE PUBLICATION zero_all FOR ALL TABLES;
    `.simple();
  });

  afterEach(async () => {
    await testDBs.drop(db);
  });

  /* eslint-disable @typescript-eslint/naming-convention */
  test('query transformation and result processing', async () => {
    const published = await getPublicationInfo(db);
    const queryHandler = new QueryHandler(published.tables);

    const ast: AST = {
      select: [
        [['issues', 'id'], 'id'],
        [['issues', 'title'], 'title'],
        [['owner', 'name'], 'owner'],
        [['parent', 'title'], 'parent_title'],
        [['parent', 'owner'], 'parent_owner'],
      ],
      orderBy: [
        [['issues', 'id'], 'desc'],
        [['issues', 'title'], 'desc'],
      ],
      table: 'issues',
      joins: [
        {
          type: 'inner',
          other: {table: 'users'},
          as: 'owner',
          on: [
            ['issues', 'owner_id'],
            ['owner', 'id'],
          ],
        },
        {
          type: 'inner',
          other: {
            select: [
              [['issues', 'id'], 'issues_id'],
              [['issues', 'title'], 'title'],
              [['owner', 'name'], 'owner'],
            ],
            table: 'issues',
            joins: [
              {
                type: 'inner',
                other: {table: 'users'},
                as: 'owner',
                on: [
                  ['issues', 'owner_id'],
                  ['owner', 'id'],
                ],
              },
            ],
          },
          as: 'parent',
          on: [
            ['issues', 'parent_id'],
            ['parent', 'issues_id'],
          ],
        },
      ],
    };

    // Explanatory:
    //   This is the original query and what the results need to be when
    //   executing on the client.
    const original = new Normalized(ast).query();
    expect(original.query).toBe(
      'SELECT ' +
        'issues.id AS id, issues.title AS title, owner.name AS owner, ' +
        'parent.owner AS parent_owner, parent.title AS parent_title FROM issues ' +
        'INNER JOIN users AS owner ON issues.owner_id = owner.id ' +
        'INNER JOIN (SELECT issues.id AS issues_id, issues.title AS title, owner.name AS owner FROM issues ' +
        'INNER JOIN users AS owner ON issues.owner_id = owner.id) ' +
        'AS parent ON issues.parent_id = parent.issues_id ' +
        'ORDER BY issues.id desc, issues.title desc',
    );
    expect(await db.unsafe(original.query, original.values)).toEqual([
      {
        id: '4',
        title: 'bar',
        owner: 'Bob',
        parent_title: 'parent issue bar',
        parent_owner: 'Bob',
      },
      {
        id: '3',
        title: 'foo',
        owner: 'Candice',
        parent_title: 'parent issue foo',
        parent_owner: 'Alice',
      },
    ]);

    const equivalentAST: AST = {
      ...ast,
      select: [
        [['issues', 'id'], 'different_id_alias'],
        [['issues', 'title'], 'different_title_alias'],
        [['owner', 'name'], 'different_owner_alias'],
        [['parent', 'title'], 'parent_title'],
        [['parent', 'owner'], 'parent_owner'],
      ],
    };

    const transformed = queryHandler.transform([
      {id: 'queryHash', ast},
      {id: 'queryHash2', ast: equivalentAST},
    ]);
    const first = transformed.values().next();
    assert(!first.done);

    const {queryIDs, transformedAST, columnAliases} = first.value;
    expect(queryIDs).toEqual(['queryHash', 'queryHash2']);
    const expanded = transformedAST.query();
    const resultParser = queryHandler.resultParser(
      'foo-cvr',
      queryIDs,
      columnAliases,
    );
    const results = await db.unsafe(expanded.query, expanded.values);

    // This is what gets synced to the client (contents) and stored in the CVR (record).
    expect(resultParser.parseResults(results)).toEqual(
      new Map([
        [
          '/vs/cvr/foo-cvr/d/r/e3jqcp8k60hejdhju08414x2z',
          {
            contents: {id: '3', owner_id: '102', parent_id: '1', title: 'foo'},
            record: {
              id: {schema: 'public', table: 'issues', rowKey: {id: '3'}},
              rowVersion: '1ca',
              queriedColumns: {
                id: ['queryHash', 'queryHash2'],
                owner_id: ['queryHash', 'queryHash2'],
                parent_id: ['queryHash', 'queryHash2'],
                title: ['queryHash', 'queryHash2'],
              },
            },
          },
        ],
        [
          '/vs/cvr/foo-cvr/d/r/idb04wek62w1kiltjxjn3fxk',
          {
            contents: {id: '102', name: 'Candice'},
            record: {
              id: {schema: 'public', table: 'users', rowKey: {id: '102'}},
              rowVersion: '0c',
              queriedColumns: {
                id: ['queryHash', 'queryHash2'],
                name: ['queryHash', 'queryHash2'],
              },
            },
          },
        ],
        [
          '/vs/cvr/foo-cvr/d/r/1ngjqp2ckvs2ur64mjoacg55',
          {
            contents: {id: '1', owner_id: '100', title: 'parent issue foo'},
            record: {
              id: {schema: 'public', table: 'issues', rowKey: {id: '1'}},
              rowVersion: '1a0',
              queriedColumns: {
                id: ['queryHash', 'queryHash2'],
                owner_id: ['queryHash', 'queryHash2'],
                title: ['queryHash', 'queryHash2'],
              },
            },
          },
        ],
        [
          '/vs/cvr/foo-cvr/d/r/9483jb6yy1yq0etzidy62072z',
          {
            contents: {id: '100', name: 'Alice'},
            record: {
              id: {schema: 'public', table: 'users', rowKey: {id: '100'}},
              rowVersion: '0a',
              queriedColumns: {
                id: ['queryHash', 'queryHash2'],
                name: ['queryHash', 'queryHash2'],
              },
            },
          },
        ],
        [
          '/vs/cvr/foo-cvr/d/r/7flkrz0yskhi5ko0l0lqjccoe',
          {
            contents: {id: '4', owner_id: '101', parent_id: '2', title: 'bar'},
            record: {
              id: {schema: 'public', table: 'issues', rowKey: {id: '4'}},
              rowVersion: '1cd',
              queriedColumns: {
                id: ['queryHash', 'queryHash2'],
                owner_id: ['queryHash', 'queryHash2'],
                parent_id: ['queryHash', 'queryHash2'],
                title: ['queryHash', 'queryHash2'],
              },
            },
          },
        ],
        [
          '/vs/cvr/foo-cvr/d/r/epxvfoxp9ktkty20rjo1yyheu',
          {
            contents: {id: '101', name: 'Bob'},
            record: {
              id: {schema: 'public', table: 'users', rowKey: {id: '101'}},
              rowVersion: '0b',
              queriedColumns: {
                id: ['queryHash', 'queryHash2'],
                name: ['queryHash', 'queryHash2'],
              },
            },
          },
        ],
        [
          '/vs/cvr/foo-cvr/d/r/2z8i982skum71jkx73g5y2gao',
          {
            contents: {id: '2', owner_id: '101', title: 'parent issue bar'},
            record: {
              id: {schema: 'public', table: 'issues', rowKey: {id: '2'}},
              rowVersion: '1ab',
              queriedColumns: {
                id: ['queryHash', 'queryHash2'],
                owner_id: ['queryHash', 'queryHash2'],
                title: ['queryHash', 'queryHash2'],
              },
            },
          },
        ],
      ]),
    );
  });

  test('aggregation', async () => {
    const ast: AST = {
      table: 'issues',
      select: [[['issues', 'title'], 'title']],
      aggregate: [
        {
          aggregate: 'array',
          field: ['labels', 'name'],
          alias: 'labels',
        },
      ],
      joins: [
        {
          type: 'left',
          other: {table: 'issueLabels'},
          as: 'issueLabels',
          on: [
            ['issueLabels', 'issueId'],
            ['issues', 'id'],
          ],
        },
        {
          type: 'left',
          other: {table: 'labels'},
          as: 'labels',
          on: [
            ['labels', 'id'],
            ['issueLabels', 'labelId'],
          ],
        },
      ],
      groupBy: [['issues', 'id']],
    };

    const published = await getPublicationInfo(db);
    const queryHandler = new QueryHandler(published.tables);
    const transformed = queryHandler.transform([{id: 'queryHash', ast}]);
    const first = transformed.values().next();
    assert(!first.done);

    const {queryIDs, transformedAST, columnAliases} = first.value;
    expect(queryIDs).toEqual(['queryHash']);
    const expanded = transformedAST.query();
    const resultParser = queryHandler.resultParser(
      'foo-cvr',
      queryIDs,
      columnAliases,
    );
    const results = await db.unsafe(expanded.query, expanded.values);

    expect(resultParser.parseResults(results)).toEqual(
      new Map([
        [
          '/vs/cvr/foo-cvr/d/r/2z8i982skum71jkx73g5y2gao',
          {
            record: {
              id: {
                schema: 'public',
                table: 'issues',
                rowKey: {
                  id: '2',
                },
              },
              rowVersion: '1ab',
              queriedColumns: {
                id: ['queryHash'],
                title: ['queryHash'],
              },
            },
            contents: {
              id: '2',
              title: 'parent issue bar',
            },
          },
        ],
        [
          '/vs/cvr/foo-cvr/d/r/7flkrz0yskhi5ko0l0lqjccoe',
          {
            record: {
              id: {
                schema: 'public',
                table: 'issues',
                rowKey: {
                  id: '4',
                },
              },
              rowVersion: '1cd',
              queriedColumns: {
                id: ['queryHash'],
                title: ['queryHash'],
              },
            },
            contents: {
              id: '4',
              title: 'bar',
            },
          },
        ],
        [
          '/vs/cvr/foo-cvr/d/r/6dv4hwfdzypc4qgsin10vqzd',
          {
            record: {
              id: {
                schema: 'public',
                table: 'issueLabels',
                rowKey: {
                  id: '3',
                },
              },
              rowVersion: '1c',
              queriedColumns: {
                id: ['queryHash'],
                issueId: ['queryHash'],
                labelId: ['queryHash'],
              },
            },
            contents: {
              id: '3',
              issueId: '4',
              labelId: '2',
            },
          },
        ],
        [
          '/vs/cvr/foo-cvr/d/r/897t63zpau6rxpvh7vh6hc8lo',
          {
            record: {
              id: {
                schema: 'public',
                table: 'labels',
                rowKey: {
                  id: '2',
                },
              },
              rowVersion: '1b',
              queriedColumns: {
                id: ['queryHash'],
                name: ['queryHash'],
              },
            },
            contents: {
              id: '2',
              name: 'feature',
            },
          },
        ],
        [
          '/vs/cvr/foo-cvr/d/r/e3jqcp8k60hejdhju08414x2z',
          {
            record: {
              id: {
                schema: 'public',
                table: 'issues',
                rowKey: {
                  id: '3',
                },
              },
              rowVersion: '1ca',
              queriedColumns: {
                id: ['queryHash'],
                title: ['queryHash'],
              },
            },
            contents: {
              id: '3',
              title: 'foo',
            },
          },
        ],
        [
          '/vs/cvr/foo-cvr/d/r/57x2s4adnwpwpdu0lvbesjrhg',
          {
            record: {
              id: {
                schema: 'public',
                table: 'issueLabels',
                rowKey: {
                  id: '1',
                },
              },
              rowVersion: '1a',
              queriedColumns: {
                id: ['queryHash'],
                issueId: ['queryHash'],
                labelId: ['queryHash'],
              },
            },
            contents: {
              id: '1',
              issueId: '3',
              labelId: '1',
            },
          },
        ],
        [
          '/vs/cvr/foo-cvr/d/r/7df9ztm0wcusukm3mde11c3sj',
          {
            record: {
              id: {
                schema: 'public',
                table: 'issueLabels',
                rowKey: {
                  id: '2',
                },
              },
              rowVersion: '1b',
              queriedColumns: {
                id: ['queryHash'],
                issueId: ['queryHash'],
                labelId: ['queryHash'],
              },
            },
            contents: {
              id: '2',
              issueId: '3',
              labelId: '2',
            },
          },
        ],
        [
          '/vs/cvr/foo-cvr/d/r/1q4e262817cdj1qj69t5d31wd',
          {
            record: {
              id: {
                schema: 'public',
                table: 'labels',
                rowKey: {
                  id: '1',
                },
              },
              rowVersion: '1a',
              queriedColumns: {
                id: ['queryHash'],
                name: ['queryHash'],
              },
            },
            contents: {
              id: '1',
              name: 'bug',
            },
          },
        ],
        [
          '/vs/cvr/foo-cvr/d/r/1ngjqp2ckvs2ur64mjoacg55',
          {
            record: {
              id: {
                schema: 'public',
                table: 'issues',
                rowKey: {
                  id: '1',
                },
              },
              rowVersion: '1a0',
              queriedColumns: {
                id: ['queryHash'],
                title: ['queryHash'],
              },
            },
            contents: {
              id: '1',
              title: 'parent issue foo',
            },
          },
        ],
      ]),
    );
  });

  test('left joins which can return with null rows', async () => {
    const published = await getPublicationInfo(db);
    const queryHandler = new QueryHandler(published.tables);

    const ast: AST = {
      select: [
        [['issues', 'id'], 'id'],
        [['issues', 'title'], 'title'],
        [['owner', 'name'], 'owner'],
        [['parent', 'title'], 'parent_title'],
        [['parent', 'owner'], 'parent_owner'],
      ],
      orderBy: [
        [['issues', 'id'], 'desc'],
        [['issues', 'title'], 'desc'],
      ],
      table: 'issues',
      joins: [
        {
          type: 'inner',
          other: {table: 'users'},
          as: 'owner',
          on: [
            ['issues', 'owner_id'],
            ['owner', 'id'],
          ],
        },
        {
          type: 'left',
          other: {
            select: [
              [['issues', 'id'], 'issues_id'],
              [['issues', 'title'], 'title'],
              [['owner', 'name'], 'owner'],
            ],
            table: 'issues',
            joins: [
              {
                type: 'inner',
                other: {table: 'users'},
                as: 'owner',
                on: [
                  ['issues', 'owner_id'],
                  ['owner', 'id'],
                ],
              },
            ],
          },
          as: 'parent',
          on: [
            ['issues', 'parent_id'],
            ['parent', 'issues_id'],
          ],
        },
      ],
    };

    // Explanatory:
    //   This is the original query and what the results need to be when
    //   executing on the client.
    const original = new Normalized(ast).query();
    expect(original.query).toBe(
      'SELECT ' +
        'issues.id AS id, issues.title AS title, owner.name AS owner, ' +
        'parent.owner AS parent_owner, parent.title AS parent_title FROM issues ' +
        'INNER JOIN users AS owner ON issues.owner_id = owner.id ' +
        'LEFT JOIN (SELECT issues.id AS issues_id, issues.title AS title, owner.name AS owner FROM issues ' +
        'INNER JOIN users AS owner ON issues.owner_id = owner.id) ' +
        'AS parent ON issues.parent_id = parent.issues_id ' +
        'ORDER BY issues.id desc, issues.title desc',
    );
    expect(await db.unsafe(original.query, original.values)).toEqual([
      {
        id: '4',
        title: 'bar',
        owner: 'Bob',
        parent_title: 'parent issue bar',
        parent_owner: 'Bob',
      },
      {
        id: '3',
        title: 'foo',
        owner: 'Candice',
        parent_title: 'parent issue foo',
        parent_owner: 'Alice',
      },
      {
        id: '2',
        title: 'parent issue bar',
        owner: 'Bob',
        parent_title: null,
        parent_owner: null,
      },
      {
        id: '1',
        title: 'parent issue foo',
        owner: 'Alice',
        parent_title: null,
        parent_owner: null,
      },
    ]);

    const equivalentAST: AST = {
      ...ast,
      select: [
        [['issues', 'id'], 'different_id_alias'],
        [['issues', 'title'], 'different_title_alias'],
        [['owner', 'name'], 'different_owner_alias'],
        [['parent', 'title'], 'parent_title'],
        [['parent', 'owner'], 'parent_owner'],
      ],
    };

    const transformed = queryHandler.transform([
      {id: 'queryHash', ast},
      {id: 'queryHash2', ast: equivalentAST},
    ]);
    const first = transformed.values().next();
    assert(!first.done);

    const {queryIDs, transformedAST, columnAliases} = first.value;
    expect(queryIDs).toEqual(['queryHash', 'queryHash2']);
    const expanded = transformedAST.query();
    const resultParser = queryHandler.resultParser(
      'foo-cvr',
      queryIDs,
      columnAliases,
    );
    const results = await db.unsafe(expanded.query, expanded.values);

    // This is what gets synced to the client (contents) and stored in the CVR (record).
    expect(resultParser.parseResults(results)).toEqual(
      new Map([
        [
          '/vs/cvr/foo-cvr/d/r/e3jqcp8k60hejdhju08414x2z',
          {
            contents: {id: '3', owner_id: '102', parent_id: '1', title: 'foo'},
            record: {
              id: {schema: 'public', table: 'issues', rowKey: {id: '3'}},
              rowVersion: '1ca',
              queriedColumns: {
                id: ['queryHash', 'queryHash2'],
                owner_id: ['queryHash', 'queryHash2'],
                parent_id: ['queryHash', 'queryHash2'],
                title: ['queryHash', 'queryHash2'],
              },
            },
          },
        ],
        [
          '/vs/cvr/foo-cvr/d/r/idb04wek62w1kiltjxjn3fxk',
          {
            contents: {id: '102', name: 'Candice'},
            record: {
              id: {schema: 'public', table: 'users', rowKey: {id: '102'}},
              rowVersion: '0c',
              queriedColumns: {
                id: ['queryHash', 'queryHash2'],
                name: ['queryHash', 'queryHash2'],
              },
            },
          },
        ],
        [
          '/vs/cvr/foo-cvr/d/r/1ngjqp2ckvs2ur64mjoacg55',
          {
            contents: {
              id: '1',
              owner_id: '100',
              title: 'parent issue foo',
              parent_id: null,
            },
            record: {
              id: {schema: 'public', table: 'issues', rowKey: {id: '1'}},
              rowVersion: '1a0',
              queriedColumns: {
                id: ['queryHash', 'queryHash2'],
                owner_id: ['queryHash', 'queryHash2'],
                title: ['queryHash', 'queryHash2'],
                parent_id: ['queryHash', 'queryHash2'],
              },
            },
          },
        ],
        [
          '/vs/cvr/foo-cvr/d/r/9483jb6yy1yq0etzidy62072z',
          {
            contents: {id: '100', name: 'Alice'},
            record: {
              id: {schema: 'public', table: 'users', rowKey: {id: '100'}},
              rowVersion: '0a',
              queriedColumns: {
                id: ['queryHash', 'queryHash2'],
                name: ['queryHash', 'queryHash2'],
              },
            },
          },
        ],
        [
          '/vs/cvr/foo-cvr/d/r/7flkrz0yskhi5ko0l0lqjccoe',
          {
            contents: {id: '4', owner_id: '101', parent_id: '2', title: 'bar'},
            record: {
              id: {schema: 'public', table: 'issues', rowKey: {id: '4'}},
              rowVersion: '1cd',
              queriedColumns: {
                id: ['queryHash', 'queryHash2'],
                owner_id: ['queryHash', 'queryHash2'],
                parent_id: ['queryHash', 'queryHash2'],
                title: ['queryHash', 'queryHash2'],
              },
            },
          },
        ],
        [
          '/vs/cvr/foo-cvr/d/r/epxvfoxp9ktkty20rjo1yyheu',
          {
            contents: {id: '101', name: 'Bob'},
            record: {
              id: {schema: 'public', table: 'users', rowKey: {id: '101'}},
              rowVersion: '0b',
              queriedColumns: {
                id: ['queryHash', 'queryHash2'],
                name: ['queryHash', 'queryHash2'],
              },
            },
          },
        ],
        [
          '/vs/cvr/foo-cvr/d/r/2z8i982skum71jkx73g5y2gao',
          {
            contents: {
              id: '2',
              owner_id: '101',
              title: 'parent issue bar',
              parent_id: null,
            },
            record: {
              id: {schema: 'public', table: 'issues', rowKey: {id: '2'}},
              rowVersion: '1ab',
              queriedColumns: {
                id: ['queryHash', 'queryHash2'],
                owner_id: ['queryHash', 'queryHash2'],
                title: ['queryHash', 'queryHash2'],
                parent_id: ['queryHash', 'queryHash2'],
              },
            },
          },
        ],
      ]),
    );
  });
});
