import * as v from 'shared/src/valita.js';

const configSchema = v.object({
  ['REPLICA_ID']: v.string(),
  ['TASK_ID']: v.string().optional(),
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
});

export type Config = v.Infer<typeof configSchema>;

export function configFromEnv(): Config {
  const env = Object.fromEntries(
    Object.keys(configSchema.shape).map(key => [key, process.env[key]]),
  );
  return v.parse(env, configSchema);
}
