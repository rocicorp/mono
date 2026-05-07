import '../../../../shared/src/dotenv.ts';

import {getZeroConfig} from '../../config/zero-config.ts';
import {exitAfter} from '../../services/life-cycle.ts';
import {parentWorker, singleProcessMode} from '../../types/processes.ts';
import {createLogContext} from '../logging.ts';
import {runWorker} from './run-worker.ts';

if (!singleProcessMode()) {
  const config = getZeroConfig({env: process.env});
  const lc = createLogContext(config, 'runner');
  void exitAfter(lc, () => runWorker(parentWorker, process.env));
}

export default runWorker;
