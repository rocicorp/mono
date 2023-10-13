import {getProviderConfig} from './cf.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {publishWorker} from './publish-worker.js';

export function publishTailWorkersOptions(yargs: CommonYargsArgv) {
  return yargs;
}

type PublishTailWorkersHandlerArgs = YargvToInterface<
  ReturnType<typeof publishTailWorkersOptions>
>;

export async function publishTailWorkersHandler(
  yargs: PublishTailWorkersHandlerArgs,
): Promise<void> {
  const config = await getProviderConfig(yargs);

  await publishWorker(config, 'connections-reporter', {
    /* eslint-disable @typescript-eslint/naming-convention */
    bindings: {
      analytics_engine_datasets: [
        {
          binding: 'runningConnectionSecondsDS',
          dataset: 'RunningConnectionSeconds',
        },
      ],
    },
    /* eslint-enable @typescript-eslint/naming-convention */
  });
}
