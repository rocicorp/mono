import {consoleLogSink, LogLevel, LogSink, TeeLogSink} from '@rocicorp/logger';
import type {MutatorDefs} from 'reflect-shared/src/types.js';
import {BaseAuthDO} from './auth-do.js';
import type {AuthHandler} from './auth.js';
import type {CloseHandler} from './close-handler.js';
import type {DisconnectHandler} from './disconnect.js';
import {BaseRoomDO, getDefaultTurnDuration} from './room-do.js';
import type {RoomStartHandler} from './room-start.js';
import {extractVars} from './vars.js';
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

  closeHandler?: CloseHandler | undefined;

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

  /**
   * The max number of mutations that will be processed per turn.
   * Lowering this limit can prevent busy rooms from experiencing "overloaded"
   * exceptions at the cost of peer-to-peer latency.
   *
   * Default is `66` when `allowUnconfirmedWrites` is `false`, or `16` when
   * `allowUnconfirmedWrites` is `true`.
   */
  maxMutationsPerTurn?: number | undefined;
}

/**
 * ReflectServerOptions with some defaults and normalization applied.
 */
export type NormalizedOptions<MD extends MutatorDefs> = {
  mutators: MD;
  authHandler?: AuthHandler | undefined;
  roomStartHandler: RoomStartHandler;
  disconnectHandler: DisconnectHandler;
  closeHandler: CloseHandler;
  logSink: LogSink;
  logLevel: LogLevel;
  datadogMetricsOptions?: DatadogMetricsOptions | undefined;
  allowUnconfirmedWrites: boolean;
  maxMutationsPerTurn: number;
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
  REFLECT_API_KEY: string;
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

const noopAsync = () => Promise.resolve();

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
      roomStartHandler = noopAsync,
      disconnectHandler = noopAsync,
      closeHandler = noopAsync,
      logSinks,
      logLevel = 'debug',
      allowUnconfirmedWrites = false,
      datadogMetricsOptions = undefined,
      maxMutationsPerTurn,
    } = makeOptions(env);
    const logSink = logSinks ? combineLogSinks(logSinks) : consoleLogSink;
    return {
      mutators,
      authHandler,
      roomStartHandler,
      disconnectHandler,
      closeHandler,
      logSink,
      logLevel,
      allowUnconfirmedWrites,
      datadogMetricsOptions,
      // default to a max of 1 mutation per millisecond of turn duration
      maxMutationsPerTurn:
        maxMutationsPerTurn ?? getDefaultTurnDuration(allowUnconfirmedWrites),
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
        closeHandler,
        logSink,
        logLevel,
        allowUnconfirmedWrites,
        maxMutationsPerTurn,
      } = getOptions(env);
      super({
        mutators,
        state,
        roomStartHandler,
        disconnectHandler,
        closeHandler,
        logSink,
        logLevel,
        allowUnconfirmedWrites,
        maxMutationsPerTurn,
        env: extractVars(env),
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
        logSink,
        logLevel,
        env: extractVars(env),
      });
    }
  };
}
