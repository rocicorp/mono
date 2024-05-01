import type {LogLevel, LogSink} from '@rocicorp/logger';
import {ServiceRunnerDO} from './runner-do.js';
import {createWorker} from './worker.js';
import type {InvalidationWatcherRegistry} from './invalidation-watcher/registry.js';
import type {ServiceRunnerEnv} from './service-runner.js';

const DEFAULT_LOG_LEVEL = 'info';

const worker = createWorker((_env: ServiceRunnerEnv) => ({
  logLevel: DEFAULT_LOG_LEVEL,
  logSink: {
    log: (lc, level, message, details) => {
      console.log(lc, level, message, details);
    },
  },
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
      super({} as InvalidationWatcherRegistry, logSink, logLevel, state, env);
    }
  };
}

// eslint-disable-next-line @typescript-eslint/naming-convention
const RunnerDO = createServiceRunnerDO((env: ServiceRunnerEnv) => ({
  logLevel: env.LOG_LEVEL ?? DEFAULT_LOG_LEVEL,
  logSink: {
    log: (lc, level, message, details) => {
      console.log(lc, level, message, details);
    },
  },
}));
export {RunnerDO, worker as default};
