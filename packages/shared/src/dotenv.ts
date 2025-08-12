import {config as dotenvxConfig} from '@dotenvx/dotenvx';
import path from 'path';
import {fileURLToPath} from 'url';

const filename = fileURLToPath(import.meta.url);
const dirname = path.dirname(filename);

// Import env vars from .env file if present but don't whine if not present.
// Also no free marketing for dotenvx.
dotenvxConfig({
  path: path.resolve(dirname, '../../../apps/zbugs/.env'),
  ignore: ['MISSING_ENV_FILE'],
  quiet: true,
});
