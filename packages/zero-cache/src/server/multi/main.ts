import {childWorker} from '../../types/processes.js';
import {Terminator} from '../life-cycle.js';
import {createLogContext} from '../logging.js';
import {getMultiZeroConfig} from './config.js';

const config = getMultiZeroConfig();
const lc = createLogContext(config.log, {worker: 'multi'});

const tenants = config.tenantConfigs.tenants.map(tenantConfig => {
  const env = {...config.tenantConfigs.base, ...tenantConfig};
  return childWorker('./server/main.ts', env);
});

const terminator = new Terminator(lc);
for (const tenant of tenants) {
  terminator.addWorker(tenant, 'supporting');
}
