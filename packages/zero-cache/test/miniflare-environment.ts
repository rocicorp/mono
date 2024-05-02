// export {RunnerDO} from '../src/services/index.js';
/**
 * Using the real DO causes all sorts of errors importing
 * dependencies.
 *
 * The first issue is the `postgres` library.
 * It does have a specific build for CloudFlare which does work
 * when the environment is workerd: https://github.com/porsager/postgres/blob/cc688c642fc98c4338523d3e281e03bf0c3417b8/package.json#L11
 *
 * But once you get past that there are other libraries using
 * `require` which is not available in the CloudFlare environment.
 *
 * Throwing int he towl for now and just using a mock for
 * the miniflare tests.
 */
export class ServiceRunnerDO {
  constructor(_state: unknown, _env: unknown) {}
}
