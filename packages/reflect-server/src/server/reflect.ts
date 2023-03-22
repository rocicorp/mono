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

/**
 * ReflectServerOptions with some defaults and normalization applied.
 */
export type NormalizedOptions<MD extends MutatorDefs> = {
  mutators: MD;
  authHandler: AuthHandler;
  disconnectHandler: DisconnectHandler;
  logSink: LogSink;
  logLevel: LogLevel;
  allowUnconfirmedWrites: boolean;
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
  const roomDOClass = createRoomDOClass(makeNormalizedOptionsGetter(options));
  const authDOClass = createAuthDOClass(makeNormalizedOptionsGetter(options));
  const worker = createWorker<Env>(makeNormalizedOptionsGetter(options));

  // eslint-disable-next-line @typescript-eslint/naming-convention
  return {worker, RoomDO: roomDOClass, AuthDO: authDOClass};
}

export function createReflectServerWithoutAuthDO<
  Env extends ReflectServerBaseEnv,
  MD extends MutatorDefs,
>(
  options: (env: Env) => ReflectServerOptions<MD>,
): {
  worker: ExportedHandler<Env>;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  RoomDO: DurableObjectCtor<Env>;
} {
  const roomDOClass = createRoomDOClass(makeNormalizedOptionsGetter(options));
  const worker = createNoAuthDOWorker<Env>(
    makeNormalizedOptionsGetter(options),
  );

  // eslint-disable-next-line @typescript-eslint/naming-convention
  return {worker, RoomDO: roomDOClass};
}

const optionsPerEnv = new WeakMap<
  ReflectServerBaseEnv,
  NormalizedOptions<MutatorDefs>
>();

type GetNormalizedOptions<
  Env extends ReflectServerBaseEnv,
  MD extends MutatorDefs,
> = (env: Env) => NormalizedOptions<MD>;

function makeNormalizedOptionsGetter<
  Env extends ReflectServerBaseEnv,
  MD extends MutatorDefs,
>(
  options: ((env: Env) => ReflectServerOptions<MD>) | ReflectServerOptions<MD>,
): GetNormalizedOptions<Env, MD> {
  return (env: Env) => {
    const {
      mutators,
      authHandler,
      disconnectHandler = () => Promise.resolve(),
      logSinks,
      logLevel = 'debug',
      allowUnconfirmedWrites = false,
    } = typeof options === 'function' ? options(env) : options;
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
>(getOptions: GetNormalizedOptions<Env, MD>) {
  return class extends BaseRoomDO<MD> {
    constructor(state: DurableObjectState, env: Env) {
      const {
        mutators,
        disconnectHandler,
        logSink,
        logLevel,
        allowUnconfirmedWrites,
      } = getOptions(env);
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
>(getOptions: GetNormalizedOptions<Env, MD>) {
  return class extends BaseAuthDO {
    constructor(state: DurableObjectState, env: Env) {
      const {authHandler, logSink, logLevel} = getOptions(env);
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
