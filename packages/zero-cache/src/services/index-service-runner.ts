import type {LogLevel, LogSink} from '@rocicorp/logger';
import {createLogSink, getLogLevel} from './logging.js';
import {ServiceRunnerDO} from './runner-do.js';
import type {ServiceRunnerEnv} from './service-runner.js';
import {DurableStorage} from '../storage/durable-storage.js';

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
    constructor(storage: DurableStorage, env: Env) {
      const {logSink, logLevel} = getOptions(env);
      super(logSink, logLevel, storage, env);
    }
    async start() {
      await super.start();
    }
  };
}

const storage = new DurableStorage();
const env = process.env as unknown as ServiceRunnerEnv;
const runnerInstance = new (createServiceRunnerDO((env: ServiceRunnerEnv) => ({
  logLevel: getLogLevel(env),
  logSink: createLogSink(env),
})))(storage, env);

// Now you can call start on these instances
void runnerInstance.start();
