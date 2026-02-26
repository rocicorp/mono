import {describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {assertArray} from '../../../shared/src/asserts.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {ArrayView} from './array-view.ts';
import type {Input} from './operator.ts';
import {Join} from './join.ts';
import {consume} from './stream.ts';
import {createSource} from './test/source-factory.ts';

const lc = createSilentLogContext();

function flatSource(rows: Array<{id: number; text: string}>) {
  const ms = createSource(
    lc,
    testLogConfig,
    'items',
    {id: {type: 'number'}, text: {type: 'string'}},
    ['id'],
  );
  for (const row of rows) {
    consume(ms.push({type: 'add', row}));
  }
  return ms;
}

function flatView(ms: ReturnType<typeof flatSource>) {
  const view = new ArrayView(
    ms.connect([['id', 'asc']]),
    {singular: false, relationships: {}},
    true,
    () => {},
  );
  let data: unknown[] = [];
  view.addListener(entries => {
    assertArray(entries);
    data = [...entries];
  });
  return {view, getData: () => data};
}

type ChildRow = {id: number; parentId: number; text: string};

function parentChildSources(
  parents: Array<{id: number; name: string}>,
  children: ChildRow[],
) {
  const parentSource = createSource(
    lc,
    testLogConfig,
    'parents',
    {id: {type: 'number'}, name: {type: 'string'}},
    ['id'],
  );
  const childSource = createSource(
    lc,
    testLogConfig,
    'children',
    {id: {type: 'number'}, parentId: {type: 'number'}, text: {type: 'string'}},
    ['id'],
  );
  for (const row of parents) {
    consume(parentSource.push({type: 'add', row}));
  }
  for (const row of children) {
    consume(childSource.push({type: 'add', row}));
  }
  return {parentSource, childSource};
}

function parentChildJoin(parentInput: Input, childInput: Input) {
  return new Join({
    parent: parentInput,
    child: childInput,
    parentKey: ['id'],
    childKey: ['parentId'],
    relationshipName: 'children',
    hidden: false,
    system: 'client',
  });
}

function parentChildView(join: Input) {
  const view = new ArrayView(
    join,
    {
      singular: false,
      relationships: {children: {singular: false, relationships: {}}},
    },
    true,
    () => {},
  );
  type ParentEntry = {
    id: number;
    name: string;
    children: ChildRow[];
  };
  let data: unknown[] = [];
  view.addListener(entries => {
    assertArray(entries);
    data = [...entries];
  });
  return {view, getData: () => data, asParent: (i: number) => data[i] as ParentEntry};
}

describe('ArrayView: flat list identity preservation', () => {
  test('edit: unchanged siblings keep reference, edited row gets new reference', () => {
    const ms = flatSource([
      {id: 1, text: 'A'},
      {id: 2, text: 'B'},
      {id: 3, text: 'C'},
    ]);
    const {view, getData} = flatView(ms);
    const [refA, refB, refC] = getData();

    consume(ms.push({type: 'edit', oldRow: {id: 2, text: 'B'}, row: {id: 2, text: 'B-edited'}}));
    view.flush();

    expect(getData()[0]).toBe(refA);
    expect(getData()[1]).not.toBe(refB);
    expect(getData()[1]).toEqual(expect.objectContaining({id: 2, text: 'B-edited'}));
    expect(getData()[2]).toBe(refC);
  });

  test('add: existing rows keep reference', () => {
    const ms = flatSource([{id: 1, text: 'A'}, {id: 2, text: 'B'}]);
    const {view, getData} = flatView(ms);
    const [refA, refB] = getData();

    consume(ms.push({type: 'add', row: {id: 3, text: 'C'}}));
    view.flush();

    expect(getData()[0]).toBe(refA);
    expect(getData()[1]).toBe(refB);
    expect(getData()).toHaveLength(3);
  });

  test('remove: remaining rows keep reference', () => {
    const ms = flatSource([
      {id: 1, text: 'A'},
      {id: 2, text: 'B'},
      {id: 3, text: 'C'},
    ]);
    const {view, getData} = flatView(ms);
    const [refA, , refC] = getData();

    consume(ms.push({type: 'remove', row: {id: 2, text: 'B'}}));
    view.flush();

    expect(getData()).toHaveLength(2);
    expect(getData()[0]).toBe(refA);
    expect(getData()[1]).toBe(refC);
  });

  test('multiple pushes before single flush preserve identity correctly', () => {
    const ms = flatSource([
      {id: 1, text: 'A'},
      {id: 2, text: 'B'},
      {id: 3, text: 'C'},
    ]);
    const {view, getData} = flatView(ms);
    const [refA, , refC] = getData();

    consume(ms.push({type: 'edit', oldRow: {id: 2, text: 'B'}, row: {id: 2, text: 'B-edited'}}));
    consume(ms.push({type: 'add', row: {id: 4, text: 'D'}}));
    view.flush();

    expect(getData()[0]).toBe(refA);
    expect(getData()[1]).toEqual(expect.objectContaining({id: 2, text: 'B-edited'}));
    expect(getData()[2]).toBe(refC);
    expect(getData()).toHaveLength(4);
  });
});

describe('ArrayView: child changes bubble new references up to ancestors', () => {
  test('child edit gives parent a new reference; unrelated parent keeps reference', () => {
    const {parentSource, childSource} = parentChildSources(
      [{id: 1, name: 'parent1'}, {id: 2, name: 'parent2'}],
      [
        {id: 10, parentId: 1, text: 'child1'},
        {id: 11, parentId: 1, text: 'child2'},
        {id: 12, parentId: 2, text: 'child3'},
      ],
    );
    const join = parentChildJoin(
      parentSource.connect([['id', 'asc']]),
      childSource.connect([['id', 'asc']]),
    );
    const {view, getData, asParent} = parentChildView(join);

    const refParent1 = getData()[0];
    const refParent2 = getData()[1];
    const refChild1 = asParent(0).children[0];
    const refChild2 = asParent(0).children[1];
    const refChild3 = asParent(1).children[0];
    const refChildrenArray = asParent(0).children;

    consume(childSource.push({
      type: 'edit',
      oldRow: {id: 10, parentId: 1, text: 'child1'},
      row: {id: 10, parentId: 1, text: 'child1-edited'},
    }));
    view.flush();

    // Parent1 MUST have new ref (descendant changed)
    expect(getData()[0]).not.toBe(refParent1);
    // Children array MUST have new ref
    expect(asParent(0).children).not.toBe(refChildrenArray);
    // Edited child MUST have new ref
    expect(asParent(0).children[0]).not.toBe(refChild1);
    expect(asParent(0).children[0]).toEqual(
      expect.objectContaining({id: 10, text: 'child1-edited'}),
    );

    // Unchanged sibling child keeps ref
    expect(asParent(0).children[1]).toBe(refChild2);
    // Unrelated parent2 keeps ref
    expect(getData()[1]).toBe(refParent2);
    // Unrelated child3 keeps ref
    expect(asParent(1).children[0]).toBe(refChild3);
  });

  test('child add gives parent a new reference; existing children keep reference', () => {
    const {parentSource, childSource} = parentChildSources(
      [{id: 1, name: 'parent1'}],
      [{id: 10, parentId: 1, text: 'child1'}],
    );
    const join = parentChildJoin(
      parentSource.connect([['id', 'asc']]),
      childSource.connect([['id', 'asc']]),
    );
    const {view, getData, asParent} = parentChildView(join);

    const refParent = getData()[0];
    const refChild1 = asParent(0).children[0];

    consume(childSource.push({type: 'add', row: {id: 11, parentId: 1, text: 'child2'}}));
    view.flush();

    expect(getData()[0]).not.toBe(refParent);
    expect(asParent(0).children).toHaveLength(2);
    expect(asParent(0).children[0]).toBe(refChild1);
  });

  test('child remove gives parent a new reference; remaining children keep reference', () => {
    const {parentSource, childSource} = parentChildSources(
      [{id: 1, name: 'parent1'}],
      [
        {id: 10, parentId: 1, text: 'child1'},
        {id: 11, parentId: 1, text: 'child2'},
      ],
    );
    const join = parentChildJoin(
      parentSource.connect([['id', 'asc']]),
      childSource.connect([['id', 'asc']]),
    );
    const {view, getData, asParent} = parentChildView(join);

    const refParent = getData()[0];
    const refChild2 = asParent(0).children[1];

    consume(childSource.push({type: 'remove', row: {id: 10, parentId: 1, text: 'child1'}}));
    view.flush();

    expect(getData()[0]).not.toBe(refParent);
    expect(asParent(0).children).toHaveLength(1);
    expect(asParent(0).children[0]).toBe(refChild2);
  });

  test('grandchild edit bubbles new reference through all 3 levels', () => {
    // grandparent → parent → child (3-level chained Join)
    const gpSource = createSource(
      lc, testLogConfig, 'grandparents',
      {id: {type: 'number'}, name: {type: 'string'}}, ['id'],
    );
    const pSource = createSource(
      lc, testLogConfig, 'parents',
      {id: {type: 'number'}, gpId: {type: 'number'}, name: {type: 'string'}}, ['id'],
    );
    const cSource = createSource(
      lc, testLogConfig, 'children',
      {id: {type: 'number'}, pId: {type: 'number'}, text: {type: 'string'}}, ['id'],
    );

    consume(gpSource.push({type: 'add', row: {id: 1, name: 'gp1'}}));
    consume(gpSource.push({type: 'add', row: {id: 2, name: 'gp2'}}));
    consume(pSource.push({type: 'add', row: {id: 10, gpId: 1, name: 'p1'}}));
    consume(cSource.push({type: 'add', row: {id: 100, pId: 10, text: 'c1'}}));
    consume(cSource.push({type: 'add', row: {id: 101, pId: 10, text: 'c2'}}));

    const pcJoin = new Join({
      parent: pSource.connect([['id', 'asc']]),
      child: cSource.connect([['id', 'asc']]),
      parentKey: ['id'],
      childKey: ['pId'],
      relationshipName: 'children',
      hidden: false,
      system: 'client',
    });
    const gpJoin = new Join({
      parent: gpSource.connect([['id', 'asc']]),
      child: pcJoin,
      parentKey: ['id'],
      childKey: ['gpId'],
      relationshipName: 'parents',
      hidden: false,
      system: 'client',
    });

    type GpEntry = {id: number; name: string; parents: Array<{id: number; children: Array<{id: number; text: string}>}>};
    const view = new ArrayView(
      gpJoin,
      {singular: false, relationships: {parents: {singular: false, relationships: {children: {singular: false, relationships: {}}}}}},
      true,
      () => {},
    );
    let data: unknown[] = [];
    view.addListener(entries => {
      assertArray(entries);
      data = [...entries];
    });

    const refGp1 = data[0];
    const refGp2 = data[1];
    const gp1 = data[0] as GpEntry;
    const refP1 = gp1.parents[0];
    const refC1 = gp1.parents[0].children[0];
    const refC2 = gp1.parents[0].children[1];

    consume(cSource.push({
      type: 'edit',
      oldRow: {id: 100, pId: 10, text: 'c1'},
      row: {id: 100, pId: 10, text: 'c1-EDITED'},
    }));
    view.flush();

    const newGp1 = data[0] as GpEntry;

    // Entire ancestor chain gets new references
    expect(data[0]).not.toBe(refGp1);
    expect(newGp1.parents[0]).not.toBe(refP1);
    expect(newGp1.parents[0].children[0]).not.toBe(refC1);
    expect(newGp1.parents[0].children[0]).toEqual(expect.objectContaining({text: 'c1-EDITED'}));

    // Unchanged nodes keep references
    expect(newGp1.parents[0].children[1]).toBe(refC2);
    expect(data[1]).toBe(refGp2);
  });
});

describe('ArrayView: flush and data behavior', () => {
  test('listener fires exactly once per flush, not per push', () => {
    const ms = flatSource([{id: 1, text: 'A'}]);
    const {view} = flatView(ms);

    let callCount = 0;
    view.addListener(() => { callCount++; });
    expect(callCount).toBe(1);

    for (let i = 2; i <= 6; i++) {
      consume(ms.push({type: 'add', row: {id: i, text: String(i)}}));
    }
    expect(callCount).toBe(1);

    view.flush();
    expect(callCount).toBe(2);
  });

  test('.data reflects changes immediately without flush (no buffering)', () => {
    const ms = flatSource([{id: 1, text: 'A'}]);
    const {view} = flatView(ms);

    consume(ms.push({type: 'add', row: {id: 2, text: 'B'}}));

    assertArray(view.data);
    expect(view.data).toHaveLength(2);
    expect(view.data[1]).toEqual(expect.objectContaining({id: 2, text: 'B'}));
  });
});
