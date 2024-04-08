import {nanoid} from 'nanoid';
import {Replicache, TEST_LICENSE_KEY} from 'replicache';
import {expect, test} from 'vitest';
import {z} from 'zod';
import {generate} from '../../generate.js';
import {SetSource} from '../ivm/source/set-source.js';
import {EntityQuery} from '../query/entity-query.js';
import {makeReplicacheContext} from './replicache-context.js';

const e1 = z.object({
  id: z.string(),
  str: z.string(),
  optStr: z.string().optional(),
});

type E1 = z.infer<typeof e1>;

const {
  init: initE1,
  set: setE1,
  update: updateE1,
  delete: deleteE1,
} = generate<E1>('e1', e1.parse);

const mutators = {
  initE1,
  setE1,
  updateE1,
  deleteE1,
};

const newRep = () =>
  new Replicache({
    licenseKey: TEST_LICENSE_KEY,
    name: nanoid(),
    mutators,
  });

test('getSource - no ordering', async () => {
  const r = newRep();
  const context = makeReplicacheContext(r);
  const source = context.getSource('e1');
  expect(source).toBeDefined();

  await r.mutate.initE1({id: '1', str: 'a'});
  await r.mutate.initE1({id: '3', str: 'a'});
  await r.mutate.initE1({id: '2', str: 'a'});

  // source is ordered by id
  expect([...(source as unknown as SetSource<E1>).value]).toEqual([
    {id: '1', str: 'a'},
    {id: '2', str: 'a'},
    {id: '3', str: 'a'},
  ]);

  await r.mutate.deleteE1('1');

  expect([...(source as unknown as SetSource<E1>).value]).toEqual([
    {id: '2', str: 'a'},
    {id: '3', str: 'a'},
  ]);

  await r.mutate.updateE1({id: '3', str: 'z'});

  expect([...(source as unknown as SetSource<E1>).value]).toEqual([
    {id: '2', str: 'a'},
    {id: '3', str: 'z'},
  ]);
});

test('getSource - with ordering', async () => {
  const r = newRep();
  const context = makeReplicacheContext(r);
  const source = context.getSource('e1', [['str', 'id'], 'asc']);

  await r.mutate.initE1({id: '1', str: 'c'});
  await r.mutate.initE1({id: '3', str: 'z'});
  await r.mutate.initE1({id: '2', str: 'a'});

  expect([...(source as unknown as SetSource<E1>).value]).toEqual([
    {id: '2', str: 'a'},
    {id: '1', str: 'c'},
    {id: '3', str: 'z'},
  ]);

  const sourceDesc = context.getSource('e1', [['str', 'id'], 'desc']);
  // asc/desc don't matter. For desc we just iterate the asc collection backwards.
  expect(source).toBe(sourceDesc);

  await r.mutate.deleteE1('1');
  await r.mutate.deleteE1('3');

  expect([...(source as unknown as SetSource<E1>).value]).toEqual([
    {id: '2', str: 'a'},
  ]);

  await r.mutate.updateE1({id: '2', str: 'z'});

  expect([...(source as unknown as SetSource<E1>).value]).toEqual([
    {id: '2', str: 'z'},
  ]);
});

declare function setTimeout(callback: () => void, ms: number): number;

test('derived sources are correctly seeded', async () => {
  const r = newRep();
  const context = makeReplicacheContext(r);

  await r.mutate.initE1({id: '1', str: 'c'});
  await r.mutate.initE1({id: '3', str: 'z'});
  await r.mutate.initE1({id: '2', str: 'a'});

  const s1 = context.getSource('e1', [['str', 'id'], 'asc']);
  const s2 = context.getSource('e1');

  // go on to the next tick of the event loop
  // so our read to load the source can complete.
  // Maybe we need something on the source to expose
  // whether or not it is done seeding.
  await new Promise<void>(resolve => {
    setTimeout(resolve, 0);
  });

  expect([...(s1 as unknown as SetSource<E1>).value]).toEqual([
    {id: '2', str: 'a'},
    {id: '1', str: 'c'},
    {id: '3', str: 'z'},
  ]);
  expect([...(s2 as unknown as SetSource<E1>).value]).toEqual([
    {id: '1', str: 'c'},
    {id: '2', str: 'a'},
    {id: '3', str: 'z'},
  ]);
});

// Here as a sanity check for now.
// Full e2e integration test suite will be in `zql/index.test.ts`
test('ZQL query with Replicache', async () => {
  const r = newRep();
  const context = makeReplicacheContext(r);

  const q = new EntityQuery<E1>(context, 'e1');

  const view = q.select('id').where('str', '>', 'm').prepare().view();

  await Promise.all([
    r.mutate.initE1({id: '1', str: 'c'}),
    r.mutate.initE1({id: '3', str: 'z'}),
    r.mutate.initE1({id: '2', str: 'a'}),
    r.mutate.initE1({id: '4', str: 'x'}),
    r.mutate.initE1({id: '5', str: 'y'}),
  ]);

  expect(view.value).toEqual([{id: '3'}, {id: '4'}, {id: '5'}]);
});
