import {bench, describe} from 'vitest';
import {makeComparator} from './data.ts';
import type {SourceSchema} from './schema.ts';
import {
  applyChange,
  applyChanges,
  type ViewChange,
} from './view-apply-change.ts';
import type {Entry, Format} from './view.ts';

const schema: SourceSchema = {
  tableName: 'item',
  columns: {
    id: {type: 'number'},
    name: {type: 'string'},
  },
  primaryKey: ['id'],
  sort: [['id', 'asc']],
  system: 'client',
  relationships: {},
  isHidden: false,
  compareRows: makeComparator([['id', 'asc']]),
};

const format: Format = {
  singular: false,
  relationships: {},
};

const relationship = 'items';

function makeAddChange(id: number): ViewChange {
  return {
    type: 'add',
    node: {
      row: {id, name: `item-${id}`},
      relationships: {},
    },
  };
}

function makeChanges(n: number): ViewChange[] {
  const changes: ViewChange[] = [];
  for (let i = 0; i < n; i++) {
    changes.push(makeAddChange(i));
  }
  return changes;
}

function freshParent(): Entry {
  return {[relationship]: []};
}

// Test threshold crossover: at what N does batch beat sequential?
const sizes = [2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 25, 30, 50, 100];

const changesPerSize = new Map<number, ViewChange[]>();
for (const n of sizes) {
  changesPerSize.set(n, makeChanges(n));
}

for (const n of sizes) {
  const changes = changesPerSize.get(n)!;

  describe(`${n} changes: threshold crossover`, () => {
    bench('sequential (applyChange loop)', () => {
      let parent = freshParent();
      for (const change of changes) {
        parent = applyChange(parent, change, schema, relationship, format);
      }
    });

    bench('batch (applyChanges)', () => {
      const parent = freshParent();
      applyChanges(parent, changes, schema, relationship, format);
    });
  });
}
