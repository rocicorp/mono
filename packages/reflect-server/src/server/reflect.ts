import {consoleLogSink, LogLevel, LogSink, TeeLogSink} from '@rocicorp/logger';
import type {MutatorDefs} from 'replicache';
import {BaseAuthDO} from './auth-do.js';
import type {AuthHandler} from './auth.js';
import type {RoomStartHandler} from './room-start.js';
import type {DisconnectHandler} from './disconnect.js';
import {BaseRoomDO} from './room-do.js';
import {createWorker} from './worker.js';

export type DatadogMetricsOptions = {
  apiKey: string;
  service?: string | undefined;
};

export interface ReflectServerOptions<MD extends MutatorDefs> {
  mutators: MD;
  authHandler?: AuthHandler | undefined;

  roomStartHandler?: RoomStartHandler | undefined;

  disconnectHandler?: DisconnectHandler | undefined;

  /**
   * Where to send logs. By default logs are sent to `console.log`.
   */
  logSinks?: LogSink[] | undefined;

  /**
   * The level to log at. By default the level is 'info'.
   */
  logLevel?: LogLevel | undefined;

  /**
   * Options for reporting metrics to Datadog. By default metrics are sent nowhere.
   */
  datadogMetricsOptions?: DatadogMetricsOptions | undefined;

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
  authHandler?: AuthHandler | undefined;
  roomStartHandler: RoomStartHandler;
  disconnectHandler: DisconnectHandler;
  logSink: LogSink;
  logLevel: LogLevel;
  datadogMetricsOptions?: DatadogMetricsOptions | undefined;
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
  // eslint-disable-next-line @typescript-eslint/naming-convention
  REFLECT_AUTH_API_KEY: string;
}

export type DurableObjectCtor<Env> = new (
  state: DurableObjectState,
  env: Env,
) => DurableObject;

/**
 * Creates the different parts of a reflect server.
 * @param makeOptions Function for creating the options for the server.
 *     IMPORTANT: Do not cache the return value from this function (or any of
 *     its parts, ie a log sink) across invocations. You should return a brand
 *     new instance each time this function is called.
 *     TODO: Add reference to CF bug.
 */
export function createReflectServer<
  Env extends ReflectServerBaseEnv,
  MD extends MutatorDefs,
>(
  makeOptions: (env: Env) => ReflectServerOptions<MD>,
): {
  worker: ExportedHandler<Env>;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  RoomDO: DurableObjectCtor<Env>;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  AuthDO: DurableObjectCtor<Env>;
} {
  const normalizedOptionsGetter = makeNormalizedOptionsGetter(makeOptions);
  const roomDOClass = createRoomDOClass(normalizedOptionsGetter);
  const authDOClass = createAuthDOClass(normalizedOptionsGetter);
  const worker = createWorker<Env>(normalizedOptionsGetter);

  // eslint-disable-next-line @typescript-eslint/naming-convention
  return {worker, RoomDO: roomDOClass, AuthDO: authDOClass};
}

type GetNormalizedOptions<
  Env extends ReflectServerBaseEnv,
  MD extends MutatorDefs,
> = (env: Env) => NormalizedOptions<MD>;

function makeNormalizedOptionsGetter<
  Env extends ReflectServerBaseEnv,
  MD extends MutatorDefs,
>(
  makeOptions: (env: Env) => ReflectServerOptions<MD>,
): (env: Env) => NormalizedOptions<MD> {
  return (env: Env) => {
    const {
      mutators,
      authHandler,
      roomStartHandler = () => Promise.resolve(),
      disconnectHandler = () => Promise.resolve(),
      logSinks,
      logLevel = 'debug',
      allowUnconfirmedWrites = false,
      datadogMetricsOptions = undefined,
    } = makeOptions(env);
    const logSink = logSinks ? combineLogSinks(logSinks) : consoleLogSink;
    return {
      mutators,
      authHandler,
      roomStartHandler,
      disconnectHandler,
      logSink,
      logLevel,
      allowUnconfirmedWrites,
      datadogMetricsOptions,
    };
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
        roomStartHandler,
        disconnectHandler,
        logSink,
        logLevel,
        allowUnconfirmedWrites,
      } = getOptions(env);
      super({
        mutators,
        state,
        roomStartHandler,
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
