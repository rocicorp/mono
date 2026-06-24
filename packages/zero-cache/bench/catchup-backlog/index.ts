/* oxlint-disable no-console */
import {runHandoffBenchmark} from './handoff.ts';
import {formatReport} from './report.ts';

const results = await runHandoffBenchmark();
console.log(formatReport(results));
