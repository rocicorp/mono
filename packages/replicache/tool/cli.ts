#!/usr/bin/env node

// If developing Replicache locally, run `npm link` to link the
// command. Then you can run this command with
// `npx replicache get-license`.

import * as licensingCLI from '@rocicorp/licensing/out/cli';
import {PROD_LICENSE_SERVER_URL} from '@rocicorp/licensing/out/client';

// FYI argv[0] is node.
if (process.argv.length < 3 || process.argv[2] !== 'get-license') {
  console.error('Usage: npx replicache get-license');
  process.exit(1);
}

// TODO(phritz): add staging flag

licensingCLI.main(PROD_LICENSE_SERVER_URL);
