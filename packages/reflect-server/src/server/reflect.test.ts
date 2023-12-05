import {expect, test} from '@jest/globals';
import {assert} from 'shared/src/asserts.js';
import {TestLogSink} from '../util/test-utils.js';
import {TestDurableObjectState, TestExecutionContext} from './do-test-utils.js';
import {
  ReflectServerOptions,
  createReflectServer,
  type ReflectServerBaseEnv,
} from './reflect.js';

test('Make sure makeOptions only see the env added using reflect env', async () => {
  type Env = unknown;
  const envs: Env[] = [];
  const options = (env: Env): ReflectServerOptions<Record<never, never>> => {
    envs.push(env);
    return {
      mutators: {},
    };
  };
  // eslint-disable-next-line @typescript-eslint/naming-convention
  const {AuthDO} = createReflectServer(options);

  const {authDO, roomDO} = getMiniflareBindings();

  expect(envs.length).toEqual(0);

  const env = {
    ['REFLECT_API_KEY']: '1',
    authDO,
    roomDO,
    ['REFLECT_VAR_RU']: 'here',
    ['RANDOM_FOO']: 'bar',
  };
  const authDOID = authDO.idFromName('auth');
  new AuthDO(
    new TestDurableObjectState(
      authDOID,
      await getMiniflareDurableObjectStorage(authDOID),
    ),
    env,
  );
  expect(envs.length).toEqual(1);
  expect(envs[0]).toEqual({['RU']: 'here'});
});

test('Make sure makeOptions is called every time DO is constructed or worker fetch is called', async () => {
  const testLogSink = new TestLogSink();
  type Env = unknown;
  const envs: Env[] = [];
  const options = (env: Env): ReflectServerOptions<Record<never, never>> => {
    envs.push(env);
    return {
      logSinks: [testLogSink],
      logLevel: 'debug',
      mutators: {},
      authHandler: () => Promise.resolve({userID: 'abc'}),
      allowUnconfirmedWrites: false,
      disconnectHandler: () => Promise.resolve(),
    };
  };
  // eslint-disable-next-line @typescript-eslint/naming-convention
  const {worker, AuthDO, RoomDO} = createReflectServer(options);

  const {authDO, roomDO} = getMiniflareBindings();

  expect(envs.length).toEqual(0);

  // eslint-disable-next-line @typescript-eslint/naming-convention
  const env1 = {REFLECT_API_KEY: '1'} as ReflectServerBaseEnv;
  const authDOID = authDO.idFromName('auth');
  new AuthDO(
    new TestDurableObjectState(
      authDOID,
      await getMiniflareDurableObjectStorage(authDOID),
    ),
    env1,
  );
  expect(envs.length).toEqual(1);
  expect(envs[0]).toEqual({});
  new AuthDO(
    new TestDurableObjectState(
      authDOID,
      await getMiniflareDurableObjectStorage(authDOID),
    ),
    env1,
  );
  expect(envs.length).toEqual(2);
  expect(envs[1]).toEqual({});

  const roomDOID1 = roomDO.idFromName('room1');
  new RoomDO(
    new TestDurableObjectState(
      roomDOID1,
      await getMiniflareDurableObjectStorage(roomDOID1),
    ),
    env1,
  );
  expect(envs.length).toEqual(3);
  expect(envs[2]).toEqual({});

  const roomDOID2 = roomDO.idFromName('room2');
  new RoomDO(
    new TestDurableObjectState(
      roomDOID2,
      await getMiniflareDurableObjectStorage(roomDOID2),
    ),
    env1,
  );
  expect(envs.length).toEqual(4);
  expect(envs[3]).toEqual({});

  assert(worker.fetch);
  await worker.fetch(
    new Request('https://reflect.app/unknown'),
    env1,
    new TestExecutionContext(),
  );
  expect(envs.length).toEqual(5);
  expect(envs[4]).toEqual({});

  await worker.fetch(
    new Request('https://reflect.app/unknown'),
    env1,
    new TestExecutionContext(),
  );
  expect(envs.length).toEqual(6);
  expect(envs[5]).toEqual({});

  const env2 = {
    ['REFLECT_API_KEY']: '2',
    ['REFLECT_VAR_x']: 'was here!',
    roomDO,
    authDO,
  };
  new AuthDO(
    new TestDurableObjectState(
      authDOID,
      await getMiniflareDurableObjectStorage(authDOID),
    ),
    env2,
  );
  expect(envs.length).toEqual(7);
  expect(envs[6]).toEqual({x: 'was here!'});

  new RoomDO(
    new TestDurableObjectState(
      roomDOID1,
      await getMiniflareDurableObjectStorage(roomDOID1),
    ),
    env2,
  );
  expect(envs.length).toEqual(8);
  expect(envs[7]).toEqual({x: 'was here!'});

  await worker.fetch(
    new Request('https://reflect.app/unknown'),
    env2,
    new TestExecutionContext(),
  );
  expect(envs.length).toEqual(9);
  expect(envs[8]).toEqual({x: 'was here!'});
});
