import {expect, suite, test} from 'vitest';
import {Snitch, type SnitchMessage} from './snitch.js';
import {MemorySource} from './memory-source.js';
import {Join} from './join.js';
import {MemoryStorage} from './memory-storage.js';
import {Catch} from './catch.js';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.js';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.js';
import type {Row} from '../../../zero-protocol/src/data.js';
import type {Ordering} from '../../../zero-protocol/src/ast.js';
import type {Node} from './data.js';
import {Exists} from './exists.js';

const base = {
  columns: [
    {id: {type: 'string'}},
    {id: {type: 'string'}, issueID: {type: 'string'}},
  ],
  primaryKeys: [['id'], ['id']],
  join: {
    parentKey: 'id',
    childKey: 'issueID',
    relationshipName: 'comments',
  },
} as const;

const oneParentWithChildTest: FetchTest = {
  ...base,
  name: 'one parent with child',
  existsType: 'EXISTS',
  sources: [[{id: 'i1'}], [{id: 'c1', issueID: 'i1'}]],
  expectedMessages: {
    initialFetch: [
      ['0', 'fetch', {}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
      ['0', 'fetch', {start: {row: {id: 'i1'}, basis: 'at'}}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
    ],
    fetch: [
      ['0', 'fetch', {}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
    ],
    cleanup: [
      ['0', 'cleanup', {}],
      ['1', 'cleanup', {constraint: {key: 'issueID', value: 'i1'}}],
    ],
  },
  expectedStorage: {
    '["size",["i1"]]': 1,
  },
  expectedHydrate: [
    {
      row: {id: 'i1'},
      relationships: {
        comments: [{row: {id: 'c1', issueID: 'i1'}, relationships: {}}],
      },
    },
  ],
};

const oneParentNoChildTest: FetchTest = {
  ...base,
  name: 'one parent no child',
  sources: [[{id: 'i1'}], []],
  existsType: 'EXISTS',
  expectedMessages: {
    initialFetch: [
      ['0', 'fetch', {}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
      ['0', 'fetch', {start: {row: {id: 'i1'}, basis: 'at'}}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
    ],
    fetch: [
      ['0', 'fetch', {}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
    ],
    cleanup: [
      ['0', 'cleanup', {}],
      ['1', 'cleanup', {constraint: {key: 'issueID', value: 'i1'}}],
    ],
  },
  expectedStorage: {
    '["size",["i1"]]': 0,
  },
  expectedHydrate: [],
};

const threeParentsTwoWithChildrenTest: FetchTest = {
  ...base,
  name: 'three parents, two with children',
  sources: [
    [{id: 'i1'}, {id: 'i2'}, {id: 'i3'}],
    [
      {id: 'c1', issueID: 'i1'},
      {id: 'c2', issueID: 'i3'},
    ],
  ],
  existsType: 'EXISTS',
  expectedMessages: {
    initialFetch: [
      ['0', 'fetch', {}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
      ['0', 'fetch', {start: {row: {id: 'i1'}, basis: 'at'}}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i2'}}],
      ['0', 'fetch', {start: {row: {id: 'i2'}, basis: 'at'}}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i2'}}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i3'}}],
      ['0', 'fetch', {start: {row: {id: 'i3'}, basis: 'at'}}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i3'}}],
    ],
    fetch: [
      ['0', 'fetch', {}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i2'}}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i3'}}],
    ],
    cleanup: [
      ['0', 'cleanup', {}],
      ['1', 'cleanup', {constraint: {key: 'issueID', value: 'i1'}}],
      ['1', 'cleanup', {constraint: {key: 'issueID', value: 'i2'}}],
      ['1', 'cleanup', {constraint: {key: 'issueID', value: 'i3'}}],
    ],
  },
  expectedStorage: {
    '["size",["i1"]]': 1,
    '["size",["i2"]]': 0,
    '["size",["i3"]]': 1,
  },
  expectedHydrate: [
    {
      row: {id: 'i1'},
      relationships: {
        comments: [{row: {id: 'c1', issueID: 'i1'}, relationships: {}}],
      },
    },
    {
      row: {id: 'i3'},
      relationships: {
        comments: [{row: {id: 'c2', issueID: 'i3'}, relationships: {}}],
      },
    },
  ],
};

const threeParentsNoChildrenTest: FetchTest = {
  ...base,
  name: 'three parents no children',
  sources: [[{id: 'i1'}, {id: 'i2'}, {id: 'i3'}], []],
  existsType: 'EXISTS',
  expectedMessages: {
    initialFetch: [
      ['0', 'fetch', {}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
      ['0', 'fetch', {start: {row: {id: 'i1'}, basis: 'at'}}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i2'}}],
      ['0', 'fetch', {start: {row: {id: 'i2'}, basis: 'at'}}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i2'}}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i3'}}],
      ['0', 'fetch', {start: {row: {id: 'i3'}, basis: 'at'}}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i3'}}],
    ],
    fetch: [
      ['0', 'fetch', {}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i1'}}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i2'}}],
      ['1', 'fetch', {constraint: {key: 'issueID', value: 'i3'}}],
    ],
    cleanup: [
      ['0', 'cleanup', {}],
      ['1', 'cleanup', {constraint: {key: 'issueID', value: 'i1'}}],
      ['1', 'cleanup', {constraint: {key: 'issueID', value: 'i2'}}],
      ['1', 'cleanup', {constraint: {key: 'issueID', value: 'i3'}}],
    ],
  },
  expectedStorage: {
    '["size",["i1"]]': 0,
    '["size",["i2"]]': 0,
    '["size",["i3"]]': 0,
  },
  expectedHydrate: [],
};

suite('EXISTS', () => {
  fetchTest(oneParentWithChildTest);
  fetchTest(oneParentNoChildTest);
  fetchTest(threeParentsTwoWithChildrenTest);
  fetchTest(threeParentsNoChildrenTest);
});

suite('NOT EXISTS', () => {
  fetchTest({
    ...oneParentWithChildTest,
    existsType: 'NOT EXISTS',
    expectedHydrate: [],
  });
  fetchTest({
    ...oneParentNoChildTest,
    existsType: 'NOT EXISTS',
    expectedHydrate: [
      {
        row: {id: 'i1'},
        relationships: {comments: []},
      },
    ],
  });
  fetchTest({
    ...threeParentsTwoWithChildrenTest,
    existsType: 'NOT EXISTS',
    expectedHydrate: [
      {
        row: {id: 'i2'},
        relationships: {comments: []},
      },
    ],
  });
  fetchTest({
    ...threeParentsNoChildrenTest,
    existsType: 'NOT EXISTS',
    expectedHydrate: [
      {
        row: {id: 'i1'},
        relationships: {comments: []},
      },
      {
        row: {id: 'i2'},
        relationships: {comments: []},
      },
      {
        row: {id: 'i3'},
        relationships: {comments: []},
      },
    ],
  });
});

// This test runs the join through three phases:
// initial fetch, fetch, and cleanup.
function fetchTest(t: FetchTest) {
  test(t.name, () => {
    const log: SnitchMessage[] = [];

    const sources = t.sources.map((rows, i) => {
      const ordering = t.sorts?.[i] ?? [['id', 'asc']];
      const source = new MemorySource(`t${i}`, t.columns[i], t.primaryKeys[i]);
      for (const row of rows) {
        source.push({type: 'add', row});
      }
      const snitch = new Snitch(source.connect(ordering), String(i), log);
      return {
        source,
        snitch,
      };
    });

    const existsStorage = new MemoryStorage();
    const exists = new Exists(
      new Join({
        parent: sources[0].snitch,
        child: sources[1].snitch,
        storage: new MemoryStorage(),
        ...t.join,
        hidden: false,
      }),
      existsStorage,
      t.join.relationshipName,
      t.existsType,
    );

    for (const [method, fetchType] of [
      ['fetch', 'initialFetch'],
      ['fetch', 'fetch'],
      ['cleanup', 'cleanup'],
    ] as const) {
      log.length = 0;

      const c = new Catch(exists);
      const r = c[method]();

      expect(r).toEqual(t.expectedHydrate);
      expect(c.pushes).toEqual([]);

      if (method === 'fetch') {
        expect(existsStorage.cloneData()).toEqual(t.expectedStorage);
      } else {
        method satisfies 'cleanup';
        expect(existsStorage.cloneData()).toEqual({});
      }

      const expectedMessages = t.expectedMessages[fetchType];
      expect(log, fetchType).toEqual(expectedMessages);
    }
  });
}

type FetchTest = {
  name: string;
  columns: readonly Record<string, SchemaValue>[];
  primaryKeys: readonly PrimaryKey[];
  sources: readonly Row[][];
  sorts?: (Ordering | undefined)[] | undefined;
  join: {
    parentKey: string;
    childKey: string;
    relationshipName: string;
  };
  existsType: 'EXISTS' | 'NOT EXISTS';
  expectedMessages: {
    initialFetch: SnitchMessage[];
    fetch: SnitchMessage[];
    cleanup: SnitchMessage[];
  };
  expectedStorage: Record<string, number>;
  expectedHydrate: Node[];
};
