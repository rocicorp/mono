import {
  createReflectServer,
  ReflectServerBaseEnv,
  newOptionsBuilder,
  datadogLogging,
  datadogMetrics,
  logLevel,
  defaultConsoleLogSink,
} from '@rocicorp/reflect/server';
import {ensureNotBotController} from '../alive/client-model';
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

const DEFAULT_LOG_LEVEL = 'info';
const DEFAULT_DATADOG_SERVICE_LABEL = 'reflect.net';

const {
  worker,
  // eslint-disable-next-line @typescript-eslint/naming-convention
  RoomDO: SuperRoomDO,
  // eslint-disable-next-line @typescript-eslint/naming-convention
  AuthDO,
} = createReflectServer(
  newOptionsBuilder((_: ReflectNetServerEnv) => ({
    mutators,
    disconnectHandler: async tx => {
      console.log('deleting old client', tx.clientID);
      await ensureNotBotController(tx, tx.clientID);
    },
    maxMutationsPerTurn: 100,
  }))
    .add(logLevel(DEFAULT_LOG_LEVEL))
    .add(defaultConsoleLogSink())
    .add(datadogLogging(DEFAULT_DATADOG_SERVICE_LABEL))
    .add(datadogMetrics(DEFAULT_DATADOG_SERVICE_LABEL))
    .build(),
);

class RoomDO extends SuperRoomDO {
  constructor(state: any, env: ReflectNetServerEnv) {
    super(state, env);
  }
}

export {RoomDO, AuthDO};
export {worker as default};
