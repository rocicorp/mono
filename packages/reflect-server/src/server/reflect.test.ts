import {expect, test} from '@jest/globals';
import {TestLogSink} from '../util/test-utils.js';
import {
  makeNormalizedOptionsGetter,
  type ReflectServerBaseEnv,
} from './reflect.js';

test('Make sure options getter only gets called once', () => {
  const testLogSink = new TestLogSink();
  type Env = unknown;
  const envs: Env[] = [];
  const getOptions = makeNormalizedOptionsGetter((env: Env) => {
    envs.push(env);
    return {
      logSinks: [testLogSink],
      logLevel: 'debug',
      mutators: {},
      authHandler: () => Promise.resolve({userID: 'abc'}),
      allowUnconfirmedWrites: false,
      disconnectHandler: () => Promise.resolve(),
    };
  });

  // eslint-disable-next-line @typescript-eslint/naming-convention
  const env1 = {REFLECT_AUTH_API_KEY: '1'} as ReflectServerBaseEnv;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  const env2 = {REFLECT_AUTH_API_KEY: '2'} as ReflectServerBaseEnv;
  const options1 = getOptions(env1);
  const options2 = getOptions(env2);
  expect(options1).toBe(options2);
  expect(envs.length).toBe(1);
  expect(envs[0]).toBe(env1);
  expect(testLogSink.messages).toEqual([
    ['info', 'get options called with different env'],
  ]);

  // New createReflectServer "call".
  envs.length = 0;
  testLogSink.messages.length = 0;
  const getOptions2 = makeNormalizedOptionsGetter((env: Env) => {
    envs.push(env);
    return {
      logSinks: [testLogSink],
      logLevel: 'debug',
      mutators: {},
      authHandler: () => Promise.resolve({userID: 'abc'}),
      allowUnconfirmedWrites: false,
      disconnectHandler: () => Promise.resolve(),
    };
  });
  const options3 = getOptions2(env2);
  const options4 = getOptions2(env1);
  expect(options3).toBe(options4);
  expect(options1).not.toBe(options3);

  expect(envs.length).toBe(1);
  expect(envs[0]).toBe(env2);
  expect(testLogSink.messages).toEqual([
    ['info', 'get options called with different env'],
  ]);
});
