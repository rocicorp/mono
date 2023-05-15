import {consoleLogSink, LogLevel, LogSink, TeeLogSink} from '@rocicorp/logger';
import type {MutatorDefs} from 'replicache';
import {BaseAuthDO} from './auth-do.js';
import type {AuthHandler} from './auth.js';
import type {RoomStartHandler} from './room-start.js';
import type {DisconnectHandler} from './disconnect.js';
import {BaseRoomDO} from './room-do.js';
import {createWorker, MetricsSink} from './worker.js';

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
   * Where to send metrics. By default metrics are sent nowhere. A Datadog implementation
   * exists at {@link createDatadogMetricsSink}.
   */
  metricsSink?: MetricsSink | undefined;

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
  metricsSink?: MetricsSink | undefined;
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

/**
 * Creates the different parts of a reflect server.
 * @param options The options for the server. If you need access to the `Env`
 * you can use a function form. When using a function form, the function may
 * be called multiple times so it should be idempotent.
 */
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
  const normalizedOptionsGetter = makeNormalizedOptionsGetter(options);
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

// exported for testing.
export function makeNormalizedOptionsGetter<
  Env extends ReflectServerBaseEnv,
  MD extends MutatorDefs,
>(
  options: ((env: Env) => ReflectServerOptions<MD>) | ReflectServerOptions<MD>,
): (env: Env) => NormalizedOptions<MD> {
  let normalizedOptions: NormalizedOptions<MD> | undefined;
  let originalEnv: Env | undefined;
  let logSink: LogSink;
  return (env: Env) => {
    if (normalizedOptions) {
      if (originalEnv !== env) {
        logSink.log('info', 'get options called with different env');
      }
      return normalizedOptions;
    }
    originalEnv = env;
    const {
      mutators,
      authHandler,
      roomStartHandler = () => Promise.resolve(),
      disconnectHandler = () => Promise.resolve(),
      logSinks,
      logLevel = 'debug',
      allowUnconfirmedWrites = false,
      metricsSink = undefined,
    } = typeof options === 'function' ? options(env) : options;
    logSink = logSinks ? combineLogSinks(logSinks) : consoleLogSink;
    normalizedOptions = {
      mutators,
      authHandler,
      roomStartHandler,
      disconnectHandler,
      logSink,
      logLevel,
      allowUnconfirmedWrites,
      metricsSink,
    };
    return normalizedOptions;
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
