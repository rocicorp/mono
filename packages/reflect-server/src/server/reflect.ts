import type {MutatorDefs} from 'replicache';
import {consoleLogSink, LogSink, LogLevel, TeeLogSink} from '@rocicorp/logger';
import type {AuthHandler} from './auth.js';
import {BaseAuthDO} from './auth-do.js';
import {BaseRoomDO} from './room-do.js';
import {createWorker} from './worker.js';
import type {DisconnectHandler} from './disconnect.js';
import {createNoAuthDOWorker} from './no-auth-do-worker.js';

export interface ReflectServerOptions<
  Env extends ReflectServerBaseEnv,
  MD extends MutatorDefs,
> {
  mutators: MD;
  authHandler: AuthHandler;

  disconnectHandler?: DisconnectHandler | undefined;

  /**
   * Get the log sink(s). This takes an `Env` so that the log sink can depend on the environment.
   */
  getLogSinks?: ((env: Env) => LogSink[]) | undefined;

  /**
   * Get the log level. This takes an `Env` so that the log level can depend on the environment.
   */
  getLogLevel?: ((env: Env) => LogLevel) | undefined;

  /**
   * If true, outgoing network messages are sent before the writes
   * they reflect are confirmed to be durable. This enables
   * lower latency but can result in clients losing some mutations
   * in the case of an untimely server restart.
   *
   * Default is false.
   */
  allowUnconfirmedWrites?: boolean | undefined;
}

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
  options: ReflectServerOptions<Env, MD>,
): {
  worker: ExportedHandler<Env>;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  RoomDO: DurableObjectCtor<Env>;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  AuthDO: DurableObjectCtor<Env>;
} {
  const optionsWithDefaults = getOptionsWithDefaults(options);
  const roomDOClass = createRoomDOClass(optionsWithDefaults);
  const authDOClass = createAuthDOClass(optionsWithDefaults);

  const {getLogSinks, getLogLevel} = optionsWithDefaults;
  const worker = createWorker<Env>({
    getLogSink: env => combineLogSinks(getLogSinks(env)),
    getLogLevel,
  });

  // eslint-disable-next-line @typescript-eslint/naming-convention
  return {worker, RoomDO: roomDOClass, AuthDO: authDOClass};
}

export function createReflectServerWithoutAuthDO<
  Env extends ReflectServerBaseEnv,
  MD extends MutatorDefs,
>(
  options: ReflectServerOptions<Env, MD>,
): {
  worker: ExportedHandler<Env>;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  RoomDO: DurableObjectCtor<Env>;
} {
  const optionsWithDefaults = getOptionsWithDefaults(options);
  const roomDOClass = createRoomDOClass(optionsWithDefaults);
  const {authHandler, getLogSinks, getLogLevel} = optionsWithDefaults;
  const worker = createNoAuthDOWorker<Env>({
    getLogSink: env => combineLogSinks(getLogSinks(env)),
    getLogLevel,
    authHandler,
  });

  // eslint-disable-next-line @typescript-eslint/naming-convention
  return {worker, RoomDO: roomDOClass};
}

function getOptionsWithDefaults<
  Env extends ReflectServerBaseEnv,
  MD extends MutatorDefs,
>(
  options: ReflectServerOptions<Env, MD>,
): Required<ReflectServerOptions<Env, MD>> {
  const {
    disconnectHandler = () => Promise.resolve(),
    getLogSinks = _env => [consoleLogSink],
    getLogLevel = _env => 'debug',
    allowUnconfirmedWrites = false,
  } = options;
  return {
    ...options,
    disconnectHandler,
    getLogSinks,
    getLogLevel,
    allowUnconfirmedWrites,
  };
}

function createRoomDOClass<
  Env extends ReflectServerBaseEnv,
  MD extends MutatorDefs,
>(optionsWithDefaults: Required<ReflectServerOptions<Env, MD>>) {
  const {
    mutators,
    disconnectHandler,
    getLogSinks,
    getLogLevel,
    allowUnconfirmedWrites,
  } = optionsWithDefaults;
  return class extends BaseRoomDO<MD> {
    constructor(state: DurableObjectState, env: Env) {
      super({
        mutators,
        state,
        disconnectHandler,
        authApiKey: getAPIKey(env),
        logSink: combineLogSinks(getLogSinks(env)),
        logLevel: getLogLevel(env),
        allowUnconfirmedWrites,
      });
    }
  };
}

function createAuthDOClass<
  Env extends ReflectServerBaseEnv,
  MD extends MutatorDefs,
>(optionsWithDefaults: Required<ReflectServerOptions<Env, MD>>) {
  const {authHandler, getLogSinks, getLogLevel} = optionsWithDefaults;
  return class extends BaseAuthDO {
    constructor(state: DurableObjectState, env: Env) {
      super({
        roomDO: env.roomDO,
        state,
        authHandler,
        authApiKey: getAPIKey(env),
        logSink: combineLogSinks(getLogSinks(env)),
        logLevel: getLogLevel(env),
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
