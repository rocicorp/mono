import {
  consoleLogSink,
  createReflectServer,
  createWorkerDatadogLogSink,
  ReflectServerBaseEnv,
} from '@rocicorp/reflect/server';
import {deleteClient} from '../alive/client-model';
import {mutators} from '../shared/mutators';

import {version} from '@rocicorp/reflect';
console.log(version);

type ReflectNetServerEnv = {
  // eslint-disable-next-line @typescript-eslint/naming-convention
  NEW_ROOM_SECRET?: string;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  CLEAN_ROOM_UID?: string;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  DATADOG_METRICS_API_KEY?: string;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  DATADOG_LOGS_API_KEY?: string;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  DATADOG_SERVICE_LABEL?: string;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  LOG_LEVEL?: string; // should be 'error', 'debug', or 'info'
} & ReflectServerBaseEnv;

function getDatadogMetricsOptions(env: ReflectNetServerEnv) {
  if (env.DATADOG_METRICS_API_KEY === undefined) {
    console.warn(
      'Not enabling datadog metrics because env.DATADOG_METRICS_API_KEY is undefined',
    );
    return undefined;
  }
  return {
    apiKey: env.DATADOG_METRICS_API_KEY,
    service: env.DATADOG_SERVICE_LABEL ?? DEFAULT_DATADOG_SERVICE_LABEL,
  };
}

function getLogSinks(env: ReflectNetServerEnv) {
  console.log('env.DATADOG_LOGS_API_KEY', env.DATADOG_LOGS_API_KEY);
  if (env.DATADOG_LOGS_API_KEY === undefined) {
    console.warn(
      'Not enabling datadog logging because env.DATADOG_LOGS_API_KEY is undefined',
    );
    return undefined;
  }
  return [
    createWorkerDatadogLogSink({
      apiKey: env.DATADOG_LOGS_API_KEY,
      service: env.DATADOG_SERVICE_LABEL ?? DEFAULT_DATADOG_SERVICE_LABEL,
    }),
    consoleLogSink,
  ];
}

const DEFAULT_LOG_LEVEL = 'info';
function getLogLevel(env: ReflectNetServerEnv) {
  const envLogLevel = env.LOG_LEVEL;
  switch (envLogLevel) {
    case 'error':
    case 'info':
    case 'debug':
      return envLogLevel;
    case undefined:
      return DEFAULT_LOG_LEVEL;
    default:
      console.log(
        'bad log level env variable value:',
        envLogLevel,
        'defaulting to:',
        DEFAULT_LOG_LEVEL,
      );
      return DEFAULT_LOG_LEVEL;
  }
}

const DEFAULT_DATADOG_SERVICE_LABEL = 'reflect.net';
const {
  worker,
  // eslint-disable-next-line @typescript-eslint/naming-convention
  RoomDO: SuperRoomDO,
  // eslint-disable-next-line @typescript-eslint/naming-convention
  AuthDO,
} = createReflectServer((env: ReflectNetServerEnv) => ({
  mutators,
  datadogMetricsOptions: getDatadogMetricsOptions(env),
  logSinks: getLogSinks(env),
  logLevel: getLogLevel(env),
  disconnectHandler: async tx => {
    console.log('deleting old client', tx.clientID);
    await deleteClient(tx, tx.clientID);
  },
}));

class RoomDO extends SuperRoomDO {
  constructor(state: any, env: ReflectNetServerEnv) {
    super(state, env);
  }
}

export {RoomDO, AuthDO};
export {worker as default};
