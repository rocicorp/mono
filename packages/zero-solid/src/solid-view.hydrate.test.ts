/**
 * Finding #7 from the `Stream` -> `PullStream` conversion.
 *
 * `SolidView` hydrates with `drainPull(...)`, which materializes the whole
 * fetch into an array before applying any change. `ArrayView`, converted in
 * the same change, kept the lazy loop. `stream.ts`'s own `drainPullMap` doc
 * warns about this: expanding a node's relationships triggers child fetches,
 * so draining first and mapping second reorders those fetches relative to the
 * parent scan. The two views are fed identical inputs here.
 */
import {createStore} from 'solid-js/store';
import {expect, test, vi} from 'vitest';
import {ArrayView} from '../../zql/src/ivm/array-view.ts';
import {makeAddChange} from '../../zql/src/ivm/change.ts';
import type {Node} from '../../zql/src/ivm/data.ts';
import type {FetchRequest, Input} from '../../zql/src/ivm/operator.ts';
import type {SourceSchema} from '../../zql/src/ivm/schema.ts';
import {pullOf, type PullStream} from '../../zql/src/ivm/stream.ts';
import type {Format} from '../../zql/src/ivm/view.ts';
import {SolidView, type State} from './solid-view.ts';

const childSchema: SourceSchema = {
  tableName: 'child',
  columns: {id: {type: 'number'}, parentID: {type: 'number'}},
  primaryKey: ['id'],
  relationships: {},
  isHidden: false,
  system: 'client',
  compareRows: (a, b) => (a.id as number) - (b.id as number),
  sort: [['id', 'asc']],
};

const parentSchema: SourceSchema = {
  tableName: 'parent',
  columns: {id: {type: 'number'}},
  primaryKey: ['id'],
  relationships: {children: childSchema},
  isHidden: false,
  system: 'client',
  compareRows: (a, b) => (a.id as number) - (b.id as number),
  sort: [['id', 'asc']],
};

const format: Format = {
  singular: false,
  relationships: {children: {singular: false, relationships: {}}},
};

/**
 * Two parents, each with one child. Every pull of the parent scan and every
 * expansion of a relationship appends to `log`, so the interleaving of the two
 * is directly observable.
 */
function instrumentedInput(log: string[]): Input & {closed: () => boolean} {
  let closed = false;
  const parent = (id: number): Node => ({
    row: {id},
    relationships: {
      children: () => {
        log.push(`child:${id}`);
        return pullOf<Node | 'yield'>([
          {row: {id: id * 10, parentID: id}, relationships: {}},
        ]);
      },
    },
  });

  return {
    setOutput: vi.fn(),
    destroy: vi.fn(),
    getSchema: () => parentSchema,
    closed: () => closed,
    fetch(_req: FetchRequest): PullStream<Node | 'yield'> {
      const parents = [parent(1), parent(2)];
      let i = 0;
      return {
        next() {
          if (i < parents.length) {
            log.push(`parent:${i + 1}`);
            return parents[i++];
          }
          log.push('parent:end');
          return undefined;
        },
        close() {
          closed = true;
        },
      };
    },
  };
}

test('ArrayView expands each node relationship as the parent scan reaches it', () => {
  const log: string[] = [];
  const input = instrumentedInput(log);

  new ArrayView(input, format, true, () => {});

  expect(log).toEqual([
    'parent:1',
    'child:1',
    'parent:2',
    'child:2',
    'parent:end',
  ]);
  expect(input.closed()).toBe(true);
});

test('SolidView expands relationships in the same order as ArrayView', () => {
  const log: string[] = [];
  const input = instrumentedInput(log);
  const [, setState] = createStore<State>([{'': undefined}, {type: 'unknown'}]);

  new SolidView(
    input,
    () => {},
    format,
    () => {},
    true,
    () => {},
    setState,
    () => {},
  );

  // `drainPull` reads the whole parent scan first, so both child fetches are
  // pushed to the end.
  expect(log).toEqual([
    'parent:1',
    'child:1',
    'parent:2',
    'child:2',
    'parent:end',
  ]);
  expect(input.closed()).toBe(true);
});

test('SolidView releases a relationship stream when materialization throws', () => {
  const [, setState] = createStore<State>([{'': undefined}, {type: 'unknown'}]);
  const input: Input = {
    setOutput: vi.fn(),
    destroy: vi.fn(),
    getSchema: () => parentSchema,
    fetch: () => pullOf<Node | 'yield'>([]),
  };

  const view = new SolidView(
    input,
    () => {},
    format,
    () => {},
    true,
    () => {},
    setState,
    () => {},
  );

  // The grandchild fetch fails, so the recursive materialization throws while
  // the child stream is still open.
  let childClosed = false;
  const grandchildren: PullStream<Node | 'yield'> = {
    next() {
      throw new Error('grandchild fetch failed');
    },
    close: () => {},
  };
  const children: PullStream<Node | 'yield'> = {
    next: (() => {
      let done = false;
      return () => {
        if (done) {
          return undefined;
        }
        done = true;
        return {
          row: {id: 10, parentID: 1},
          relationships: {grandkids: () => grandchildren},
        };
      };
    })(),
    close() {
      childClosed = true;
    },
  };

  expect(() =>
    view.push(
      makeAddChange({
        row: {id: 1},
        relationships: {children: () => children},
      }),
    ),
  ).toThrow('grandchild fetch failed');

  expect(childClosed).toBe(true);
});
