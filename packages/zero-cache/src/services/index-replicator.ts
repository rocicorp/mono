import type {LogLevel, LogSink} from '@rocicorp/logger';
import {createLogSink, getLogLevel} from './logging.js';
import {ReplicatorDO as ReplicatorDOClass} from './replicator-do.js';
import type {ServiceRunnerEnv} from './service-runner.js';
import {DurableStorage} from '../storage/durable-storage.js';

type GetNormalizedOptions<Env extends ServiceRunnerEnv> = (
  env: Env,
) => NormalizedOptions;

export type NormalizedOptions = {
  logSink: LogSink;
  logLevel: LogLevel;
};

function createReplicatorDO<Env extends ServiceRunnerEnv>(
  getOptions: GetNormalizedOptions<Env>,
) {
  return class extends ReplicatorDOClass {
    constructor(storage: DurableStorage, env: Env) {
      const {logSink, logLevel} = getOptions(env);
      super(logSink, logLevel, storage, env);
    }
    async start() {
      await super.start();
    }
  };
}

const env = process.env as unknown as ServiceRunnerEnv;
const stroage = new DurableStorage();
const replicatorInstance = new (createReplicatorDO((env: ServiceRunnerEnv) => ({
  logLevel: getLogLevel(env),
  logSink: createLogSink(env),
})))(stroage, env);

void replicatorInstance.start();
