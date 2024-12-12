import {
  envSchema,
  parseOptionsAdvanced,
  type Config,
} from '../../../../shared/src/options.js';
import * as v from '../../../../shared/src/valita.js';
import {zeroOptions} from '../../config/zero-config.js';

const ENV_VAR_PREFIX = 'ZERO_';

export const multiConfigSchema = {
  ...zeroOptions,

  tenantsJSON: {
    type: v.string().optional(),
    desc: [
      `JSON encoding of per-tenant configs for running the server in multi-tenant mode:`,
      ``,
      `\\{`,
      `  /**`,
      `   * Requests to the main application {bold port} are dispatched to the first tenant`,
      `   * with a matching {bold host} and {bold path}. If both host and path are specified,`,
      `   * both must match for the request to be dispatched to that tenant.`,
      `   *`,
      `   * Requests can also be sent directly to the {bold ZERO_PORT} specified`,
      `   * in a tenant's {bold env} overrides. In this case, no host or path`,
      `   * matching is necessary.`,
      `   */`,
      `  tenants: \\{`,
      `     id: string;     // value of the "tid" context key in debug logs`,
      `     host?: string;  // case-insensitive full Host: header match`,
      `     path?: string;  // first path component, with or without leading slash`,
      ``,
      `     /**`,
      `      * Options are inherited from the main application (e.g. args and ENV) by default,`,
      `      * and are overridden by values in the tenant's {bold env} object.`,
      `      */`,
      `     env: \\{`,
      `       ZERO_REPLICA_DB_FILE: string`,
      `       ZERO_UPSTREAM_DB: string`,
      `       ...`,
      `     \\};`,
      `  \\}[];`,
      `\\}`,
    ],
  },
};

const zeroEnvSchema = envSchema(zeroOptions, ENV_VAR_PREFIX);

const tenantSchema = v.object({
  id: v.string(),
  host: v
    .string()
    .map(h => h.toLowerCase())
    .optional(),
  path: v
    .string()
    .chain(p => {
      if (p.indexOf('/', 1) >= 0) {
        return v.err(`Only a single path component may be specified: ${p}`);
      }
      return v.ok(p[0] === '/' ? p : '/' + p);
    })
    .optional(),
  env: zeroEnvSchema.partial(),
});

const tenantsSchema = v.object({
  tenants: v.array(tenantSchema),
});

export type MultiZeroConfig = v.Infer<typeof tenantsSchema> &
  Omit<Config<typeof multiConfigSchema>, 'tenantsJSON'>;

export function getMultiZeroConfig(
  processEnv: NodeJS.ProcessEnv = process.env,
  argv = process.argv.slice(2),
): {config: MultiZeroConfig; env: NodeJS.ProcessEnv} {
  const {
    config: {tenantsJSON, ...config},
    env,
  } = parseOptionsAdvanced(
    multiConfigSchema,
    argv,
    ENV_VAR_PREFIX,
    false,
    true, // allowPartial, as options can be merged with each tenant's `env`
    processEnv,
  );
  const tenantsConfig = tenantsJSON
    ? v.parse(JSON.parse(tenantsJSON), tenantsSchema)
    : {tenants: []};
  return {config: {...config, ...tenantsConfig}, env};
}
