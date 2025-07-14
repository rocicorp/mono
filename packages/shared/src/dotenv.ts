import {config as dotenvxConfig} from '@dotenvx/dotenvx';
import path from 'path';
import {fileURLToPath} from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Import env vars from .env file if present but don't whine if not present.
// Also no free marketing for dotenvx.
dotenvxConfig({
  path: path.resolve(__dirname, '../../../apps/zbugs/.env'),
  ignore: ['MISSING_ENV_FILE'],
  quiet: true,
});
