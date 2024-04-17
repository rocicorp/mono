import {generate} from '@rocicorp/rails';
import {nanoid} from 'nanoid';
import {Replicache, TEST_LICENSE_KEY} from 'replicache';
import {expect, test} from 'vitest';
import {z} from 'zod';
import type {SetSource} from '../ivm/source/set-source.js';
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
  const context = makeReplicacheContext(r, {
    subscriptionAdded() {},
    subscriptionRemoved() {},
  });
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
