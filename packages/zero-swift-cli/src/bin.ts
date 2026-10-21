#!/usr/bin/env node
import {main} from './cli.ts';

process.exitCode = await main(process.argv.slice(2));
