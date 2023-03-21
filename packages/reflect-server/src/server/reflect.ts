import {consoleLogSink, LogLevel, LogSink, TeeLogSink} from '@rocicorp/logger';
import type {MutatorDefs} from 'replicache';
import {BaseAuthDO} from './auth-do.js';
import type {AuthHandler} from './auth.js';
import type {DisconnectHandler} from './disconnect.js';
import {createNoAuthDOWorker} from './no-auth-do-worker.js';
import {BaseRoomDO} from './room-do.js';
import {createWorker} from './worker.js';

export interface ReflectServerOptions<MD extends MutatorDefs> {
  mutators: MD;
  authHandler: AuthHandler;

  disconnectHandler?: DisconnectHandler | undefined;

  /**
   * The log sinks. If you need access to the `Env` you can use a function form
   * when calling {@link createReflectServer}.
   */
  logSinks?: LogSink[] | undefined;

  /**
   * The level to log at. If you need access to the `Env` you can use a function
   * form when calling {@link createReflectServer}.
   */
  logLevel?: LogLevel | undefined;

  /**
   * If `true`, outgoing network messages are sent before the writes they
   * reflect are confirmed to be durable. This enables lower latency but can
   * result in clients losing some mutations in the case of an untimely server
   * restart.
   *
   * Default is `false`.
   */
  allowUnconfirmedWrites?: boolean | undefined;
}

type Required<T> = {
  [P in keyof T]-?: Exclude<T[P], undefined>;
};

type ReflectServerOptionsWithDefaults<MD extends MutatorDefs> = Required<
  Omit<ReflectServerOptions<MD>, 'mutators' | 'logSinks'>
> & {
  mutators: MD;
  logSink: LogSink;
};

function combineLogSinks(sinks: LogSink[]): LogSink {
  if (sinks.length === 1) {
    return sinks[0];
  }
  return new TeeLogSink(sinks);
}

export interface ReflectServerBaseEnv {
  roomDO: DurableObjectNamespace;
  authDO: DurableObjectNamespace;
  /**
   * If not bound the Auth API will be disabled.
   */
  // eslint-disable-next-line @typescript-eslint/naming-convention
  REFLECT_AUTH_API_KEY?: string;
}

export type DurableObjectCtor<Env> = new (
  state: DurableObjectState,
  env: Env,
) => DurableObject;

export function createReflectServer<
  Env extends ReflectServerBaseEnv,
  MD extends MutatorDefs,
>(
  options: ReflectServerOptions<MD> | ((env: Env) => ReflectServerOptions<MD>),
): {
  worker: ExportedHandler<Env>;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  RoomDO: DurableObjectCtor<Env>;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  AuthDO: DurableObjectCtor<Env>;
} {
  const getOptionsWithDefaults = getOptionsFuncWithDefaultsPerEnv(options);
  const roomDOClass = createRoomDOClass(getOptionsWithDefaults);
  const authDOClass = createAuthDOClass(getOptionsWithDefaults);
  const worker = createWorker<Env>(getOptionsWithDefaults);

  // eslint-disable-next-line @typescript-eslint/naming-convention
  return {worker, RoomDO: roomDOClass, AuthDO: authDOClass};
}

export function createReflectServerWithoutAuthDO<
  Env extends ReflectServerBaseEnv,
  MD extends MutatorDefs,
>(
  getOptions: (env: Env) => ReflectServerOptions<MD>,
): {
  worker: ExportedHandler<Env>;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  RoomDO: DurableObjectCtor<Env>;
} {
  const getOptionsWithDefaults = getOptionsFuncWithDefaultsPerEnv(getOptions);
  const roomDOClass = createRoomDOClass(getOptionsWithDefaults);
  const worker = createNoAuthDOWorker<Env>(getOptionsWithDefaults);

  // eslint-disable-next-line @typescript-eslint/naming-convention
  return {worker, RoomDO: roomDOClass};
}

const optionsPerEnv = new WeakMap<
  ReflectServerBaseEnv,
  ReflectServerOptionsWithDefaults<MutatorDefs>
>();

function getOptionsFuncWithDefaultsPerEnv<
  Env extends ReflectServerBaseEnv,
  MD extends MutatorDefs,
>(
  getOptions:
    | ((env: Env) => ReflectServerOptions<MD>)
    | ReflectServerOptions<MD>,
): (env: Env) => ReflectServerOptionsWithDefaults<MD> {
  return (env: Env) => {
    const existingOptions = optionsPerEnv.get(env);
    if (existingOptions) {
      return existingOptions as ReflectServerOptionsWithDefaults<MD>;
    }
    const {
      mutators,
      authHandler,
      disconnectHandler = () => Promise.resolve(),
      logSinks,
      logLevel = 'debug',
      allowUnconfirmedWrites = false,
    } = typeof getOptions === 'function' ? getOptions(env) : getOptions;
    const newOptions = {
      mutators,
      authHandler,
      disconnectHandler,
      logSink: logSinks ? combineLogSinks(logSinks) : consoleLogSink,
      logLevel,
      allowUnconfirmedWrites,
    };
    optionsPerEnv.set(env, newOptions);
    return newOptions;
  };
}
function createRoomDOClass<
  Env extends ReflectServerBaseEnv,
  MD extends MutatorDefs,
>(getOptionsWithDefaults: (env: Env) => ReflectServerOptionsWithDefaults<MD>) {
  return class extends BaseRoomDO<MD> {
    constructor(state: DurableObjectState, env: Env) {
      const {
        mutators,
        disconnectHandler,
        logSink,
        logLevel,
        allowUnconfirmedWrites,
      } = getOptionsWithDefaults(env);
      super({
        mutators,
        state,
        disconnectHandler,
        authApiKey: getAPIKey(env),
        logSink,
        logLevel,
        allowUnconfirmedWrites,
      });
    }
  };
}

function createAuthDOClass<
  Env extends ReflectServerBaseEnv,
  MD extends MutatorDefs,
>(getOptionsWithDefaults: (env: Env) => ReflectServerOptionsWithDefaults<MD>) {
  return class extends BaseAuthDO {
    constructor(state: DurableObjectState, env: Env) {
      const {authHandler, logSink, logLevel} = getOptionsWithDefaults(env);
      super({
        roomDO: env.roomDO,
        state,
        authHandler,
        authApiKey: getAPIKey(env),
        logSink,
        logLevel,
      });
    }
  };
}

function getAPIKey(env: ReflectServerBaseEnv) {
  const val = env.REFLECT_AUTH_API_KEY;
  if (!val) {
    throw new Error('REFLECT_AUTH_API_KEY environment var is required');
  }
  return val;
}
