import type {LogLevel, LogSink} from '@rocicorp/logger';
import {ServiceRunnerDO} from './runner-do.js';
import {createWorker} from './worker.js';
import type {ServiceRunnerEnv} from './service-runner.js';
import {createLogSink, getLogLevel} from './logging.js';

const worker = createWorker((env: ServiceRunnerEnv) => ({
  logLevel: getLogLevel(env),
  logSink: createLogSink(env),
}));

type GetNormalizedOptions<Env extends ServiceRunnerEnv> = (
  env: Env,
) => NormalizedOptions;

export type NormalizedOptions = {
  logSink: LogSink;
  logLevel: LogLevel;
};

function createServiceRunnerDO<Env extends ServiceRunnerEnv>(
  getOptions: GetNormalizedOptions<Env>,
) {
  return class extends ServiceRunnerDO {
    constructor(state: DurableObjectState, env: Env) {
      const {logSink, logLevel} = getOptions(env);
      super(logSink, logLevel, state, env);
    }
  };
}

// eslint-disable-next-line @typescript-eslint/naming-convention
const RunnerDO = createServiceRunnerDO((env: ServiceRunnerEnv) => ({
  logLevel: getLogLevel(env),
  logSink: createLogSink(env),
}));
export {RunnerDO, worker as default};
