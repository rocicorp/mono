const {config} = require('@dotenvx/dotenvx');
const path = require('path');

config({
  path: path.resolve(__dirname, '../../../apps/zbugs/.env'),
  ignore: ['MISSING_ENV_FILE'],
  quiet: true,
});
