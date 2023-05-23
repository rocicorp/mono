import {expect, test} from '@jest/globals';
import {assert} from 'shared/asserts.js';
import {TestLogSink} from '../util/test-utils.js';
import {TestDurableObjectState, TestExecutionContext} from './do-test-utils.js';
import {
  createReflectServer,
  type ReflectServerBaseEnv,
  ReflectServerOptions,
} from './reflect.js';

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
  const {worker, AuthDO, RoomDO} = createReflectServer(options);

  const {authDO, roomDO} = getMiniflareBindings();

  expect(envs.length).toEqual(0);

  // eslint-disable-next-line @typescript-eslint/naming-convention
  const env1 = {REFLECT_AUTH_API_KEY: '1'} as ReflectServerBaseEnv;
  const authDOID = authDO.idFromName('auth');
  new AuthDO(
    new TestDurableObjectState(
      authDOID,
      await getMiniflareDurableObjectStorage(authDOID),
    ),
    env1,
  );
  expect(envs.length).toEqual(1);
  expect(envs[0]).toBe(env1);
  new AuthDO(
    new TestDurableObjectState(
      authDOID,
      await getMiniflareDurableObjectStorage(authDOID),
    ),
    env1,
  );
  expect(envs.length).toEqual(2);
  expect(envs[1]).toBe(env1);

  const roomDOID1 = roomDO.idFromName('room1');
  new RoomDO(
    new TestDurableObjectState(
      roomDOID1,
      await getMiniflareDurableObjectStorage(roomDOID1),
    ),
    env1,
  );
  expect(envs.length).toEqual(3);
  expect(envs[2]).toBe(env1);

  const roomDOID2 = roomDO.idFromName('room2');
  new RoomDO(
    new TestDurableObjectState(
      roomDOID2,
      await getMiniflareDurableObjectStorage(roomDOID2),
    ),
    env1,
  );
  expect(envs.length).toEqual(4);
  expect(envs[3]).toBe(env1);

  assert(worker.fetch);
  await worker.fetch(
    new Request('https://reflect.app/unknown'),
    env1,
    new TestExecutionContext(),
  );
  expect(envs.length).toEqual(5);
  expect(envs[4]).toBe(env1);

  await worker.fetch(
    new Request('https://reflect.app/unknown'),
    env1,
    new TestExecutionContext(),
  );
  expect(envs.length).toEqual(6);
  expect(envs[5]).toBe(env1);

  // eslint-disable-next-line @typescript-eslint/naming-convention
  const env2 = {REFLECT_AUTH_API_KEY: '2'} as ReflectServerBaseEnv;
  new AuthDO(
    new TestDurableObjectState(
      authDOID,
      await getMiniflareDurableObjectStorage(authDOID),
    ),
    env2,
  );
  expect(envs.length).toEqual(7);
  expect(envs[6]).toBe(env2);

  new RoomDO(
    new TestDurableObjectState(
      roomDOID1,
      await getMiniflareDurableObjectStorage(roomDOID1),
    ),
    env2,
  );
  expect(envs.length).toEqual(8);
  expect(envs[7]).toBe(env2);

  await worker.fetch(
    new Request('https://reflect.app/unknown'),
    env2,
    new TestExecutionContext(),
  );
  expect(envs.length).toEqual(9);
  expect(envs[8]).toBe(env2);
});
