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

type ParentEntry = {
  id: number;
  name: string;
  children: ChildRow[];
};

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
  let data: unknown[] = [];
  view.addListener(entries => {
    assertArray(entries);
    data = [...entries];
  });
  return {view, getData: () => data, asParent: (idx: number) => data[idx] as ParentEntry};
}

describe('ArrayView: flat list identity preservation', () => {
  //   [A, B, C]  --edit B-->  [A, B', C]
  //    |     |                  |      |
  //    same  same               same   same
  //          |                         |
  //       different              new ref (edited)
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

  //   [A, B]  --add C-->  [A, B, C]
  //    |  |                 |  |
  //   same same            same same
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

  //   [A, B, C]  --remove B-->  [A, C]
  //    |     |                    |  |
  //   same  same                 same same
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

  //   [A, B, C]  --edit B, add D-->  [A, B', C, D]
  //    |     |                         |      |
  //   same  same                      same   same
  //          |                               |
  //       new ref                         new ref
  test('multiple pushes before single flush preserve identity correctly', () => {
    const ms = flatSource([
      {id: 1, text: 'A'},
      {id: 2, text: 'B'},
      {id: 3, text: 'C'},
    ]);
    const {view, getData} = flatView(ms);
    const [refA, refB, refC] = getData();

    consume(ms.push({type: 'edit', oldRow: {id: 2, text: 'B'}, row: {id: 2, text: 'B-edited'}}));
    consume(ms.push({type: 'add', row: {id: 4, text: 'D'}}));
    view.flush();

    expect(getData()[0]).toBe(refA);
    expect(getData()[1]).not.toBe(refB);
    expect(getData()[1]).toEqual(expect.objectContaining({id: 2, text: 'B-edited'}));
    expect(getData()[2]).toBe(refC);
    expect(getData()).toHaveLength(4);
  });
});

describe('ArrayView: child changes bubble new references up to ancestors', () => {
  //   parent1 ─┬─ [child1, child2]     edit child1     parent1' ─┬─ [child1', child2]
  //            │                         ────────►                │
  //   parent2 ─┴─ [child3]                              parent2  ─┴─ [child3]
  //                                                      same ref       same ref
  //
  //   parent1:  new ref (descendant changed)
  //   child1:   new ref (edited)
  //   child2:   same ref (unchanged)
  //   parent2:  same ref (unrelated)
  //   child3:   same ref (unrelated)
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

    expect(getData()[0]).not.toBe(refParent1);
    expect(asParent(0).children).not.toBe(refChildrenArray);
    expect(asParent(0).children[0]).not.toBe(refChild1);
    expect(asParent(0).children[0]).toEqual(
      expect.objectContaining({id: 10, text: 'child1-edited'}),
    );
    expect(asParent(0).children[1]).toBe(refChild2);
    expect(getData()[1]).toBe(refParent2);
    expect(asParent(1).children[0]).toBe(refChild3);
  });

  //   parent1 ─── [child1]     add child2     parent1' ─── [child1, child2]
  //                              ────────►
  //   parent1:  new ref (children changed)
  //   child1:   same ref (unchanged)
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

  //   parent1 ─── [child1, child2]     remove child1     parent1' ─── [child2]
  //                                      ────────────►
  //   parent1:  new ref (children changed)
  //   child2:   same ref (unchanged)
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

  //   gp1 ─── p1 ─┬─ c1       edit c1       gp1' ─── p1' ─┬─ c1'
  //                └─ c2        ──────►                      └─ c2  (same ref)
  //   gp2                                     gp2  (same ref)
  //
  //   gp1: new ref    p1: new ref    c1: new ref (edited)
  //   gp2: same ref                  c2: same ref (unchanged)
  test('grandchild edit bubbles new reference through all 3 levels', () => {
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

    type ChildEntry = {id: number; pId: number; text: string};
    type MidEntry = {id: number; gpId: number; name: string; children: ChildEntry[]};
    type GpEntry = {id: number; name: string; parents: MidEntry[]};

    const view = new ArrayView(
      gpJoin,
      {
        singular: false,
        relationships: {
          parents: {
            singular: false,
            relationships: {children: {singular: false, relationships: {}}},
          },
        },
      },
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

    expect(data[0]).not.toBe(refGp1);
    expect(newGp1.parents[0]).not.toBe(refP1);
    expect(newGp1.parents[0].children[0]).not.toBe(refC1);
    expect(newGp1.parents[0].children[0]).toEqual(expect.objectContaining({text: 'c1-EDITED'}));
    expect(newGp1.parents[0].children[1]).toBe(refC2);
    expect(data[1]).toBe(refGp2);
  });

  //   parent1 ─── child(.one())       edit child       parent1' ─── child'(.one())
  //                                     ────────►
  //   parent1: new ref (singular child changed)
  //   child:   new ref (edited)
  test('singular relationship: child edit gives parent a new reference', () => {
    const parentSource = createSource(
      lc, testLogConfig, 'parents',
      {id: {type: 'number'}, name: {type: 'string'}}, ['id'],
    );
    const childSource = createSource(
      lc, testLogConfig, 'children',
      {id: {type: 'number'}, parentId: {type: 'number'}, text: {type: 'string'}}, ['id'],
    );

    consume(parentSource.push({type: 'add', row: {id: 1, name: 'parent1'}}));
    consume(childSource.push({type: 'add', row: {id: 10, parentId: 1, text: 'only-child'}}));

    const join = new Join({
      parent: parentSource.connect([['id', 'asc']]),
      child: childSource.connect([['id', 'asc']]),
      parentKey: ['id'],
      childKey: ['parentId'],
      relationshipName: 'child',
      hidden: false,
      system: 'client',
    });

    const view = new ArrayView(
      join,
      {
        singular: false,
        relationships: {child: {singular: true, relationships: {}}},
      },
      true,
      () => {},
    );

    type SingularParent = {id: number; name: string; child: ChildRow | undefined};

    let data: unknown[] = [];
    view.addListener(entries => {
      assertArray(entries);
      data = [...entries];
    });

    const refParent = data[0];
    const parent = data[0] as SingularParent;
    const refChild = parent.child;
    expect(refChild).toEqual(expect.objectContaining({id: 10, text: 'only-child'}));

    consume(childSource.push({
      type: 'edit',
      oldRow: {id: 10, parentId: 1, text: 'only-child'},
      row: {id: 10, parentId: 1, text: 'only-child-EDITED'},
    }));
    view.flush();

    const newParent = data[0] as SingularParent;
    expect(data[0]).not.toBe(refParent);
    expect(newParent.child).not.toBe(refChild);
    expect(newParent.child).toEqual(expect.objectContaining({text: 'only-child-EDITED'}));
  });
});

describe('ArrayView: flush and data behavior', () => {
  //   push(A), push(B), push(C), push(D), push(E)  -->  flush()  -->  listener fires ONCE
  test('listener fires exactly once per flush, not per push', () => {
    const ms = flatSource([{id: 1, text: 'A'}]);
    const {view} = flatView(ms);

    let callCount = 0;
    view.addListener(() => { callCount++; });
    expect(callCount).toBe(1);

    for (let idx = 2; idx <= 6; idx++) {
      consume(ms.push({type: 'add', row: {id: idx, text: String(idx)}}));
    }
    expect(callCount).toBe(1);

    view.flush();
    expect(callCount).toBe(2);
  });

  //   push(add B)  -->  view.data  -->  [A, B] (no flush needed)
  test('.data reflects changes immediately without flush (no buffering)', () => {
    const ms = flatSource([{id: 1, text: 'A'}]);
    const {view} = flatView(ms);

    consume(ms.push({type: 'add', row: {id: 2, text: 'B'}}));

    assertArray(view.data);
    expect(view.data).toHaveLength(2);
    expect(view.data[1]).toEqual(expect.objectContaining({id: 2, text: 'B'}));
  });
});
