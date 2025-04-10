import {setSingleProcessMode} from '../../types/processes.ts';
import {runWorker} from '../runner/run-worker.ts';
setSingleProcessMode(true);
await runWorker(null, process.env);
