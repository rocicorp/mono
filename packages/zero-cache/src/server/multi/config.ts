import {parseOptions, type Config} from '../../../../shared/src/options.js';
import * as v from '../../../../shared/src/valita.js';
import {logOptions} from '../../config/zero-config.js';

export const tenantConfigsSchema = v.object({
  base: v.record(v.string()).optional(),
  tenants: v.array(v.record(v.string())),
});

export type TenantConfigs = v.Infer<typeof tenantConfigsSchema>;

export const multiConfigSchema = {
  tenantConfigsJSON: {
    type: v.string(),
    desc: [
      `JSON encoding TenantConfigs, which define the ENV variables`,
      `used to configure each tenant's logical zero-cache:`,
      ``,
      `\\{`,
      `  /** ENV variables inherited by all tenants, unless overridden. */`,
      `  base?: \\{[env: string]: string\\};`,
      ``,
      `  /** Tenant-specific ENV variables. */`,
      `  tenants: \\{[env: string]: string\\}[];`,
      `\\}`,
    ],
  },

  log: logOptions,
};

export type MultiZeroConfig = Config<typeof multiConfigSchema> & {
  tenantConfigs: TenantConfigs;
};

const ENV_VAR_PREFIX = 'ZERO_';

export function getMultiZeroConfig(): MultiZeroConfig {
  const config = parseOptions(
    multiConfigSchema,
    process.argv.slice(2),
    ENV_VAR_PREFIX,
  );
  return {
    ...config,
    tenantConfigs: v.parse(
      JSON.parse(config.tenantConfigsJSON),
      tenantConfigsSchema,
    ),
  };
}
