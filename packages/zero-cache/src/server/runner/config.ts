import {
  envSchema,
  parseOptionsAdvanced,
  type Config,
} from '../../../../shared/src/options.ts';
import * as v from '../../../../shared/src/valita.ts';
import {zeroOptions} from '../../config/zero-config.ts';

const ENV_VAR_PREFIX = 'ZERO_';

export const multiConfigSchema = {
  ...zeroOptions,

  serverVersion: {
    type: v.string().optional(),
    desc: [`The version string outputted to logs when the server starts up.`],
  },

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
      `     /**`,
      `      * Unique per-tenant ID used internally for multi-node dispatch.`,
      `      *`,
      `      * The ID may only contain alphanumeric characters, underscores, and hyphens.`,
      `      * Note that changing the ID may result in temporary disruption in multi-node`,
      `      * mode, when the configs in the view-syncer and replication-manager differ.`,
      `      */`,
      `     id: string;`,
      `     host?: string;  // case-insensitive full Host: header match`,
      `     path?: string;  // first path component, with or without leading slash`,
      ``,
      `     /**`,
      `      * Options are inherited from the main application (e.g. args and ENV) by default,`,
      `      * and are overridden by values in the tenant's {bold env} object.`,
      `      */`,
      `     env: \\{`,
      `       ZERO_REPLICA_FILE: string`,
      `       ZERO_UPSTREAM_DB: string`,
      `       ZERO_CVR_DB: string`,
      `       ZERO_CHANGE_DB: string`,
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
      p = p[0] === '/' ? p : '/' + p;
      return v.ok(p + '/'); // normalized to '/{path}/'
    })
    .optional(),
  env: zeroEnvSchema.partial().extend({
    // Keep this as a required field. Note that ZERO_UPSTREAM_DB is optional as
    // it can be shared provided that each tenant has its own ZERO_APP_ID.
    ['ZERO_REPLICA_FILE']: v.string(),
  }),
});

const ID_REGEX = /^[A-Za-z0-9_-]+$/;

const tenantsSchema = v
  .object({
    tenants: v.array(tenantSchema),
  })
  .chain(val => {
    const ids = new Set();
    for (const {id} of val.tenants) {
      if (!ID_REGEX.test(id)) {
        return v.err(
          `Invalid tenant ID "${id}". Must be non-empty, and contain only alphanumeric characters, underscores, and hyphens`,
        );
      }
      if (ids.has(id)) {
        return v.err(`Multiple tenants with ID ${id}`);
      }
      ids.add(id);
    }
    return v.ok(val);
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
