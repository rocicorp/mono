import * as v from 'shared/src/valita.js';

const optionalBool = v
  .string()
  .optional()
  .map(v => v !== undefined && (v === '1' || v.toLowerCase() === 'true'));

const configSchema = v.object({
  ['REPLICA_ID']: v.string(),
  ['UPSTREAM_URI']: v.string(),
  ['CVR_DB_URI']: v.string(),
  ['CHANGE_DB_URI']: v.string(),
  ['REPLICA_DB_FILE']: v.string(),
  ['STORAGE_DB_TMP_DIR']: v.string().optional(),
  ['LOG_LEVEL']: v.union(
    v.literal('debug'),
    v.literal('info'),
    v.literal('error'),
  ),
  ['DATADOG_LOGS_API_KEY']: v.string().optional(),
  ['DATADOG_SERVICE_LABEL']: v.string().optional(),
  ['APP_CONFIG_PATH']: v.string().optional(),

  // Task-orchestration config.
  ['NUM_SYNC_WORKERS']: v.string().optional(),
  ['CHANGE_STREAMER_URI']: v.string().optional(),
  ['LITESTREAM']: optionalBool,
});

export type Config = v.Infer<typeof configSchema>;

export function configFromEnv(): Config {
  const env = Object.fromEntries(
    Object.keys(configSchema.shape).map(key => [key, process.env[key]]),
  );
  return v.parse(env, configSchema);
}
