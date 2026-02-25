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

const scales = [5, 10, 50, 100, 500, 1_000, 5_000, 10_000, 50_000];

// Pre-generate so generation cost is not measured.
const changesPerScale = new Map<number, ViewChange[]>();
for (const n of scales) {
	changesPerScale.set(n, makeChanges(n));
}

for (const n of scales) {
	const changes = changesPerScale.get(n)!;

	describe(`${n} add changes`, () => {
		bench('sequential (applyChange)', () => {
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
