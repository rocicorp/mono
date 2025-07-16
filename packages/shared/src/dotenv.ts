import {config as dotenvxConfig} from '@dotenvx/dotenvx';

// Import env vars from .env file if present but don't whine if not present.
// Also no free marketing for dotenvx.
dotenvxConfig({
  ignore: ['MISSING_ENV_FILE'],
  quiet: true,
});
