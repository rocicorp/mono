import {
  envSchema,
  parseOptions,
  type Config,
} from '../../../../shared/src/options.js';
import * as v from '../../../../shared/src/valita.js';
import {logOptions, zeroOptions} from '../../config/zero-config.js';

const ENV_VAR_PREFIX = 'ZERO_';

const multiConfigSchema = {
  tenantConfigsJSON: {
    type: v.string(),
    desc: [
      `JSON encoding of TenantConfigs, which define the configuration of each`,
      `tenant's logical zero-cache:`,
      ``,
      `\\{`,
      `  /** ENV variables inherited by all tenants, unless overridden. */`,
      `  baseEnv?: \\{`,
      `    ZERO_LOG_LEVEL: string`,
      `    ZERO_LOG_FORMAT: string`,
      `    ...`,
      `  \\}`,
      ``,
      `  /**`,
      `   * Requests are dispatched to the first tenant with a matching host and path,`,
      `   * at least one of which must be specified. If both host and path are specified,`,
      `   * both must match for the request to be dispatched to that tenant.`,
      `   */`,
      `  tenants: \\{`,
      `     id: string;     // value of the "tid" context key in debug logs`,
      `     host?: string;  // case-insensitive full Host: header match`,
      `     path?: string;  // first path component, with or without leading slash`,
      ``,
      `     /** Tenant-specific ENV variables. */`,
      `     env: \\{`,
      `       ZERO_REPLICA_DB_FILE: string`,
      `       ZERO_UPSTREAM_DB: string`,
      `       ...`,
      `     \\};`,
      `  \\}[];`,
      `\\}`,
    ],
  },

  port: {
    type: v.number().default(4848),
    desc: [
      `The main port for incoming connections.`,
      `Internally, zero-cache will also listen on the two ports after {bold --port},`,
      `and may use up to 3 ports for each tenant thereafter.`,
    ],
  },

  heartbeatMonitorPort: {
    type: v.number().optional(),
    desc: [
      `The port on which the heartbeat monitor listens for heartbeat`,
      `health checks. Once health checks are received at this port,`,
      `the monitor considers it a keepalive signal and triggers a drain`,
      `if health checks stop for more than 15 seconds. If health checks`,
      `never arrive on this port, the monitor does nothing (i.e. opt-in).`,
      ``,
      `If unspecified, defaults to {bold --port} + 2.`,
    ],
  },

  log: logOptions,
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

const tenantConfigsSchema = v
  .object({
    baseEnv: zeroEnvSchema.partial().optional(),
    tenants: v.array(tenantSchema),
  })
  .chain(c => {
    const {baseEnv, ...config} = c;
    for (const tenant of config.tenants) {
      if (tenant.host === undefined && tenant.path === undefined) {
        return v.err(`Tenant "${tenant.id}" is missing a host or path field`);
      }
      const mergedEnv = v.test({...baseEnv, ...tenant.env}, zeroEnvSchema);
      if (!mergedEnv.ok) {
        return v.err(mergedEnv.error);
      }
      tenant.env = mergedEnv.value;
    }
    return v.ok(config);
  });

export type MultiZeroConfig = v.Infer<typeof tenantConfigsSchema> &
  Omit<Config<typeof multiConfigSchema>, 'tenantConfigsJSON'>;

export function getMultiZeroConfig(
  env: NodeJS.ProcessEnv = process.env,
  argv = process.argv.slice(2),
): MultiZeroConfig {
  const {tenantConfigsJSON, ...config} = parseOptions(
    multiConfigSchema,
    argv,
    ENV_VAR_PREFIX,
    env,
  );
  const merged = v.parse(JSON.parse(tenantConfigsJSON), tenantConfigsSchema);
  return {...config, ...merged};
}
