import {LogContext, type LogSink} from '@rocicorp/logger';
import {bench, describe, use} from './bench.ts';

const sink: LogSink = {
  log(): void {
    return;
  },
};

const context = {
  component: 'benchmark',
  shard: 'app_0/1',
  taskID: 'task-0',
};

const ITERATIONS = 1_000;

describe('LogContext construction', () => {
  bench('new debug context 1000x', () => {
    const contexts = new Array<LogContext>(ITERATIONS);
    for (let i = 0; i < ITERATIONS; i++) {
      contexts[i] = new LogContext('debug', context, sink);
    }
    use(contexts.at(-1)?.debug);
  });

  bench('new info context 1000x', () => {
    const contexts = new Array<LogContext>(ITERATIONS);
    for (let i = 0; i < ITERATIONS; i++) {
      contexts[i] = new LogContext('info', context, sink);
    }
    use(contexts.at(-1)?.info);
  });

  bench('new error context 1000x', () => {
    const contexts = new Array<LogContext>(ITERATIONS);
    for (let i = 0; i < ITERATIONS; i++) {
      contexts[i] = new LogContext('error', context, sink);
    }
    use(contexts.at(-1)?.error);
  });

  bench('withContext on debug context 1000x', () => {
    const root = new LogContext('debug', context, sink);
    const contexts = new Array<LogContext>(ITERATIONS);
    for (let i = 0; i < ITERATIONS; i++) {
      contexts[i] = root.withContext('iteration', i);
    }
    use(contexts.at(-1)?.debug);
  });
});
