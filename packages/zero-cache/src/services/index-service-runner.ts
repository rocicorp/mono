import type {LogLevel, LogSink} from '@rocicorp/logger';
import {createLogSink, getLogLevel} from './logging.js';
import {ServiceRunnerDO} from './runner-do.js';
import type {ServiceRunnerEnv} from './service-runner.js';

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
    constructor(env: Env) {
      const {logSink, logLevel} = getOptions(env);
      super(logSink, logLevel, env);
    }
    async start() {
      await super.start();
    }
  };
}

const env = process.env as unknown as ServiceRunnerEnv;
const runnerInstance = new (createServiceRunnerDO((env: ServiceRunnerEnv) => ({
  logLevel: getLogLevel(env),
  logSink: createLogSink(env),
})))(env);

// Now you can call start on these instances
void runnerInstance.start();
