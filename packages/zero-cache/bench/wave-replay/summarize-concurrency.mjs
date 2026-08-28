import {readFile, writeFile} from 'node:fs/promises';

const [runsPath, eventsPath, outputPath] = process.argv.slice(2);
const runs = (await readFile(runsPath, 'utf8'))
	.trim()
	.split('\n')
	.map(line => JSON.parse(line));
const events = JSON.parse(await readFile(eventsPath, 'utf8'));
const byGroup = Map.groupBy(events, event => event.clientGroupID);

const rows = [];
for (const run of runs) {
	for (const result of run.results) {
		const groupEvents = byGroup.get(result.clientGroup) ?? [];
		const queryWave = groupEvents.find(event => event.zeroEvent === 'diagnosis-query-wave-stages');
		const querySync = groupEvents.find(event => event.zeroEvent === 'diagnosis-query-sync-stages');
		const config = groupEvents.find(event => event.zeroEvent === 'diagnosis-config-update-stages');
		const tracker = groupEvents.find(
			event => event.zeroEvent === 'query-pipeline-hydrate-finish' && event.queryName === 'problem_trackers.for_assignment',
		);
		const lockReleases = groupEvents.filter(event => event.zeroEvent === 'diagnosis-view-syncer-lock-released');
		const queryLockWorkMs = Math.max(0, ...lockReleases.map(event => event.lockWorkMs));
		rows.push({
			clients: run.clients,
			run: run.run,
			clientGroup: result.clientGroup,
			settleMs: result.totalSettleSeconds * 1000,
			configMs: config?.totalMs,
			transformMs: querySync?.customTransformMs,
			queryProcessingMs: queryWave?.queryProcessingCpuMs,
			queryWaveMs: queryWave?.totalMs,
			cvrFlushMs: queryWave?.cvrFlushMs,
			pokeEndMs: queryWave?.pokeEndMs,
			trackerHydrateMs: tracker?.hydrationTimeMs,
			queryLockWorkMs,
		});
	}
}

const median = values => {
	const sorted = values.filter(Number.isFinite).toSorted((a, b) => a - b);
	const middle = Math.floor(sorted.length / 2);
	return sorted.length % 2 === 0
		? (sorted[middle - 1] + sorted[middle]) / 2
		: sorted[middle];
};

const summary = [];
for (const [clients, groupRows] of Map.groupBy(rows, row => row.clients)) {
	const fields = [
		'settleMs',
		'configMs',
		'transformMs',
		'queryProcessingMs',
		'queryWaveMs',
		'cvrFlushMs',
		'pokeEndMs',
		'trackerHydrateMs',
		'queryLockWorkMs',
	];
	const medians = Object.fromEntries(
		fields.map(field => [field, median(groupRows.map(row => row[field]))]),
	);
	summary.push({clients, samples: groupRows.length, ...medians});
}

await writeFile(outputPath, `${JSON.stringify({summary, rows}, null, 2)}\n`);
console.log(JSON.stringify(summary, null, 2));
