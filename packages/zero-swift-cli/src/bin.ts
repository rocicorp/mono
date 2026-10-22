#!/usr/bin/env node
import {cli, legacyArgv} from './cli.ts';

await cli.serve(legacyArgv(process.argv.slice(2)));
