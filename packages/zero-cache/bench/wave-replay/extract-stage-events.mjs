import {readFile, writeFile} from 'node:fs/promises';

const [inputPath, outputPath] = process.argv.slice(2);
const text = await readFile(inputPath, 'utf8');
const events = [];

for (const line of text.split('\n')) {
	if (!line.includes('diagnosis ') || !line.includes('{"zeroEvent"')) continue;
	const jsonStart = line.indexOf('{"zeroEvent"');
	try {
		events.push(JSON.parse(line.slice(jsonStart)));
	} catch {}
}

const lifecyclePattern = /^(\S+) \[\n([^\]]*)\n\] query pipeline lifecycle$/gm;
for (const match of text.matchAll(lifecyclePattern)) {
	const event = {timestamp: match[1]};
	for (const field of match[2].matchAll(/'([^'=]+)=([^']*)'/g)) {
		event[field[1]] = field[2];
	}
	for (const key of ['hydrationTimeMs', 'hydrationRowCount']) {
		if (event[key] !== undefined) event[key] = Number(event[key]);
	}
	events.push(event);
}

await writeFile(outputPath, `${JSON.stringify(events, null, 2)}\n`);
