/**
 * Sweep entrypoint that delegates to either binary capacity search
 * (binary-sweep.ts) or linear matrix comparison (linear-sweep.ts).
 */
const argv = process.argv.slice(2);
const cleanArgv = argv[0] === '--' ? argv.slice(1) : argv;

const isLinear =
  cleanArgv.includes('--mode=linear') ||
  cleanArgv.some((arg, i) => arg === '--mode' && cleanArgv[i + 1] === 'linear');

if (isLinear) {
  const filtered = cleanArgv.filter((arg, i) => {
    if (arg === '--mode=linear') {
      return false;
    }
    if (arg === '--mode') {
      return false;
    }
    if (i > 0 && cleanArgv[i - 1] === '--mode') {
      return false;
    }
    return true;
  });
  const {parseLinearSweepArgs, runLinearSweep} =
    await import('./linear-sweep.ts');
  const {sweepPoints, pointLabel, stdout} = await import('./sweep-common.ts');
  const config = parseLinearSweepArgs(filtered);
  if (config.dryRun) {
    const points = sweepPoints(config);
    stdout(`zero-throughput linear sweep dry run\n`);
    stdout(
      `points: ${points.length}, write rates: ${config.writeRates.join(', ')}\n`,
    );
    stdout(
      `total benchmark runs: ${points.length * config.writeRates.length}\n\n`,
    );
    for (const point of points) {
      for (const rate of config.writeRates) {
        stdout(`${pointLabel(point)} @ ${rate} writes/s\n`);
      }
    }
  } else {
    await runLinearSweep(config);
  }
} else {
  const {parseBinarySweepArgs, runBinarySweep} =
    await import('./binary-sweep.ts');
  const {sweepPoints, pointLabel, stdout} = await import('./sweep-common.ts');
  const config = parseBinarySweepArgs(cleanArgv);
  if (config.dryRun) {
    const points = sweepPoints(config);
    stdout(`zero-throughput binary search sweep dry run\n`);
    stdout(`points: ${points.length}\n`);
    stdout(
      `max benchmark runs: ${points.length * config.searchSteps * config.repetitions}\n`,
    );
    stdout(
      `write-rate search: ${config.writeRateMin}-${config.writeRateMax} logical writes/s, ${config.searchSteps} steps\n\n`,
    );
    for (const point of points) {
      stdout(`${pointLabel(point)}\n`);
    }
  } else {
    await runBinarySweep(config);
  }
}
