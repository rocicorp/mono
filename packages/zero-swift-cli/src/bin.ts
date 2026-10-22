#!/usr/bin/env node
import {main, UsageError} from './cli.ts';

try {
  process.exitCode = await main(process.argv.slice(2));
} catch (e) {
  if (!(e instanceof UsageError)) {
    throw e;
  }
  process.stderr.write(`zero-swift: ${e.message}\n`);
  process.exitCode = 1;
}
