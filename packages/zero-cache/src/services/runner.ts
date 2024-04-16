import type {DurableObjectState} from '@cloudflare/workers-types';

interface Env {}

export class ServiceRunnerDO {
  constructor(_state: DurableObjectState, _env: Env) {}
}
