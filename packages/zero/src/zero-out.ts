#!/usr/bin/env node

import '../../shared/src/dotenv.ts';

import {createLogContext} from '../../shared/src/logging.ts';
import {parseOptions} from '../../shared/src/options.ts';
import {ZERO_ENV_VAR_PREFIX as envNamePrefix} from '../../zero-cache/src/config/zero-config.ts';
import {
  decommissionOptions,
  decommissionZero,
} from '../../zero-cache/src/scripts/decommission.ts';

async function main() {
  const config = parseOptions(decommissionOptions, {envNamePrefix});
  const lc = createLogContext(config);
  await decommissionZero(lc, config);
}

void main();
