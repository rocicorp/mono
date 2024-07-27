import type {LogLevel, LogSink} from '@rocicorp/logger';
import {createLogSink, getLogLevel} from './logging.js';
import {Replicator} from './replicator.js';
import type {ServiceRunnerEnv} from './service-runner.js';

type GetNormalizedOptions<Env extends ServiceRunnerEnv> = (
  env: Env,
) => NormalizedOptions;

export type NormalizedOptions = {
  logSink: LogSink;
  logLevel: LogLevel;
};

function createReplicator<Env extends ServiceRunnerEnv>(
  getOptions: GetNormalizedOptions<Env>,
) {
  return class extends Replicator {
    constructor(env: Env) {
      const {logSink, logLevel} = getOptions(env);
      super(logSink, logLevel, env);
    }
  };
}

const env = process.env as unknown as ServiceRunnerEnv;
const replicatorInstance = new (createReplicator((env: ServiceRunnerEnv) => ({
  logLevel: getLogLevel(env),
  logSink: createLogSink(env),
})))(env);

void replicatorInstance.start();
