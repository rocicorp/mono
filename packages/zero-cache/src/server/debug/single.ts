import {setSingleProcessMode} from '../../types/processes.ts';
import {runWorker} from '../multi/run-worker.ts';
setSingleProcessMode(true);
await runWorker(null, process.env);
