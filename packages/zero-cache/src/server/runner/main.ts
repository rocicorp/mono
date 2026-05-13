import {consoleLogSink, LogContext} from '@rocicorp/logger';
import '../../../../shared/src/dotenv.ts';

import {exitAfter} from '../../services/life-cycle.ts';
import {parentWorker, singleProcessMode} from '../../types/processes.ts';
import {runWorker} from './run-worker.ts';

if (!singleProcessMode()) {
  void exitAfter(new LogContext('info', {}, consoleLogSink), () =>
    runWorker(parentWorker, process.env),
  );
}

export default runWorker;
