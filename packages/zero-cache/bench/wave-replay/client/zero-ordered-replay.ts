/**
 * Replay the educator assignment-detail page's first Zero query wave against a
 * configurable zero-cache, and measure per-query registration -> first data ->
 * complete.
 *
 * Hypothesis under test: registering the whole wave in one tick serializes on
 * the server per client group, so one heavy member (problem_trackers) stalls
 * unrelated roots and they all complete in lockstep. Mode `staggered` registers
 * the same queries one at a time, each awaited before the next.
 *
 * This file imports `@goblins/zero`, so bun must resolve it from inside the
 * goblins checkout. `run-replay-concurrency.ts` copies it to
 * `$GOBLINS_REPO/.tmp/zero-ordered-replay.ts` before spawning it.
 *
 *   bun .tmp/zero-ordered-replay.ts \
 *     --zero-cache-url http://localhost:49700 \
 *     --auth "<better-auth session token>" \
 *     --user-id <user id> --email <email> \
 *     --assignment-id assignment_emu_lag_136 \
 *     --mode wave|staggered|wave-control|two-tier
 */
import { create_zero_client } from "@goblins/zero/react";
import { queries } from "@goblins/zero/synced-queries";

type Mode = "wave" | "staggered" | "wave-control" | "two-tier";

const readFlag = (name: string) => {
	const index = process.argv.indexOf(`--${name}`);
	return index === -1 ? undefined : process.argv[index + 1];
};

const required = (name: string) => {
	const value = readFlag(name);
	if (value === undefined) {
		console.error(`missing --${name}`);
		process.exit(1);
	}
	return value;
};

const cacheURL = required("zero-cache-url");
const auth = required("auth");
const userID = required("user-id");
const email = readFlag("email") ?? null;
const assignmentId = required("assignment-id");
const mode = (readFlag("mode") ?? "wave") as Mode;
const timeoutMs = Number(readFlag("timeout-ms") ?? 120_000);

const zero = create_zero_client({
	userID,
	auth,
	cacheURL,
	kvStore: "mem",
	storageKey: `ordered-replay-${mode}-${Date.now()}`,
	logLevel: "error",
	queryTimeoutMs: "none",
	context: {
		user_id: userID,
		email,
		student_id: null,
		verification: { email: "complete" },
	},
});

// The exact first-wave set the route registers, in route order:
// packages/app/app/routes/educator.assignments.$assignment_code._index.tsx
// (`assignment.basic`, `assignment.summary`, `assignment.with_problems` in the
// page header; `assignment.roster`, `assignment.with_problems`,
// `problem_trackers.for_assignment` in the `useQueries` cohort at ttl 30s; the
// alerts badge's `misconduct.for_assignment_count`). `with_problems` appears
// twice in the route with identical args, so it is one registration.
const wave = [
	{
		label: "assignment.basic",
		query: queries.assignment.basic({ id: assignmentId }),
		ttl: undefined,
	},
	{
		label: "assignment.summary",
		query: queries.assignment.summary({ assignment_id: assignmentId }),
		ttl: undefined,
	},
	{
		label: "assignment.roster",
		query: queries.assignment.roster({ id: assignmentId, statuses: undefined }),
		ttl: "30s" as const,
	},
	{
		label: "assignment.with_problems",
		query: queries.assignment.with_problems({ id: assignmentId }),
		ttl: "30s" as const,
	},
	{
		label: "problem_trackers.for_assignment",
		query: queries.problem_trackers.for_assignment({
			assignment_id: assignmentId,
		}),
		ttl: "30s" as const,
	},
	{
		label: "misconduct.for_assignment_count",
		query: queries.misconduct.for_assignment_count({
			assignment_id: assignmentId,
		}),
		ttl: undefined,
	},
];

const TRACKERS = "problem_trackers.for_assignment";
const light = wave.filter((entry) => entry.label !== TRACKERS);
const trackers = wave.filter((entry) => entry.label === TRACKERS);

// Each phase registers together and is awaited before the next one starts.
const phases: Record<Mode, (typeof wave)[number][][]> = {
	wave: [wave],
	staggered: wave.map((entry) => [entry]),
	"wave-control": [light],
	"two-tier": [light, trackers],
};

type Measurement = {
	label: string;
	registeredAt: number;
	firstDataAt: number | undefined;
	completeAt: number | undefined;
	completedWallClock: string | undefined;
	rows: number;
	error: string | undefined;
};

const countRows = (data: unknown) =>
	Array.isArray(data) ? data.length : data === undefined || data === null ? 0 : 1;

let start = 0;

const register = (entry: (typeof wave)[number]) => {
	const registeredAt = performance.now() - start;
	const measurement: Measurement = {
		label: entry.label,
		registeredAt,
		firstDataAt: undefined,
		completeAt: undefined,
		completedWallClock: undefined,
		rows: 0,
		error: undefined,
	};
	const view = zero.materialize(
		entry.query,
		entry.ttl === undefined ? undefined : { ttl: entry.ttl },
	);
	const settled = Promise.withResolvers<Measurement>();
	const finish = () => {
		clearTimeout(timer);
		settled.resolve(measurement);
	};
	const timer = setTimeout(() => {
		measurement.error ??= "timeout";
		finish();
	}, timeoutMs);
	view.addListener((data, resultType, error) => {
		const now = performance.now() - start;
		const rows = countRows(data);
		if (rows > 0 && measurement.firstDataAt === undefined) {
			measurement.firstDataAt = now;
			measurement.rows = rows;
		}
		if (resultType === "error") {
			measurement.error = JSON.stringify(error).slice(0, 160);
			finish();
			return;
		}
		if (resultType === "complete" && measurement.completeAt === undefined) {
			measurement.completeAt = now;
			measurement.completedWallClock = new Date().toISOString();
			measurement.rows = rows;
			finish();
		}
	});
	return { measurement, settled: settled.promise, view };
};

const format = (value: number | undefined) =>
	value === undefined ? "--" : `${(value / 1000).toFixed(3)}s`;

const run = async () => {
	start = performance.now();
	const registrations: {
		measurement: Measurement;
		settled: Promise<Measurement>;
		view: { destroy: () => void };
	}[] = [];

	for (const phase of phases[mode]) {
		const phaseRegistrations = phase.map(register);
		registrations.push(...phaseRegistrations);
		await Promise.all(
			phaseRegistrations.map((registration) => registration.settled),
		);
	}

	const rows = registrations.map((registration) => registration.measurement);
	const header = [
		"query",
		"registered",
		"first-data",
		"complete",
		"duration",
		"rows",
		"completed-at",
	];
	const table = rows.map((row) => [
		row.label,
		format(row.registeredAt),
		format(row.firstDataAt),
		format(row.completeAt),
		row.completeAt === undefined
			? "--"
			: format(row.completeAt - row.registeredAt),
		String(row.rows),
		row.completedWallClock?.slice(11, 23) ?? (row.error ?? "--"),
	]);
	const widths = header.map((_, column) =>
		Math.max(header[column].length, ...table.map((line) => line[column].length)),
	);
	const line = (cells: string[]) =>
		cells.map((cell, column) => cell.padEnd(widths[column])).join("  ");

	console.log(
		`\nmode=${mode}  cache=${cacheURL}  assignment=${assignmentId}  client_group=${await zero.clientGroupID}`,
	);
	console.log(line(header));
	console.log(line(widths.map((width) => "-".repeat(width))));
	for (const cells of table) console.log(line(cells));

	const completions = rows
		.map((row) => row.completeAt)
		.filter((value): value is number => value !== undefined);
	const firstData = rows
		.map((row) => row.firstDataAt)
		.filter((value): value is number => value !== undefined);
	const fastest = Math.min(...completions);
	const slowest = Math.max(...completions);
	console.log(
		`\nfastest complete ${format(fastest)}  slowest complete ${format(slowest)}  spread ${format(slowest - fastest)}`,
	);
	console.log(
		`first-content ${format(Math.min(...firstData))}  total settle ${format(slowest)}`,
	);

	for (const registration of registrations) registration.view.destroy();
	await zero.close();
	process.exit(0);
};

await run();
