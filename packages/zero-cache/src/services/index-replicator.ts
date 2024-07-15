import type {LogLevel, LogSink} from '@rocicorp/logger';
import {createLogSink, getLogLevel} from './logging.js';
import {ReplicatorDO as ReplicatorDOClass} from './replicator-do.js';
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
  return class extends ReplicatorDOClass {
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
const replicatorInstance = new (createReplicator((env: ServiceRunnerEnv) => ({
  logLevel: getLogLevel(env),
  logSink: createLogSink(env),
})))(env);

void replicatorInstance.start();
