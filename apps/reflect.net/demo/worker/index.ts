import {
  consoleLogSink,
  createDatadogMetricsSink,
  createReflectServer,
  createWorkerDatadogLogSink,
  ReflectServerBaseEnv,
} from '@rocicorp/reflect-server';
import {mutators} from '../shared/mutators';
import {deleteClient} from '../alive/client-model';

type ReflectNetServerEnv = {
  NEW_ROOM_SECRET?: string;
  CLEAN_ROOM_UID?: string;
  DATADOG_METRICS_API_KEY?: string;
  DATADOG_LOGS_API_KEY?: string;
} & ReflectServerBaseEnv;

function getMetricsSink(env: ReflectNetServerEnv) {
  if (env.DATADOG_METRICS_API_KEY === undefined) {
    console.warn(
      'Not enabling datadog metrics because env.DATADOG_METRICS_API_KEY is undefined',
    );
    return undefined;
  }
  return createDatadogMetricsSink({
    apiKey: env.DATADOG_METRICS_API_KEY,
    service: DATADOG_SERVICE_LABEL,
  });
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
      service: DATADOG_SERVICE_LABEL,
    }),
    consoleLogSink,
  ];
}

const DATADOG_SERVICE_LABEL = 'reflect.net';
const {
  worker,
  RoomDO: SuperRoomDO,
  AuthDO,
} = createReflectServer((env: ReflectNetServerEnv) => ({
  mutators,
  metricsSink: getMetricsSink(env),
  logSinks: getLogSinks(env),
  logLevel: 'info',
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
export default worker;
