import fs from 'node:fs';
import {Session} from 'node:inspector/promises';

const session = new Session();
session.connect();
await session.post('Profiler.enable');
await session.post('Profiler.setSamplingInterval', {interval: 1000});
await session.post('Profiler.start');

const durationMs = Number(process.env.PROFILE_MS ?? 60000);
const outPath = process.env.PROFILE_OUT ?? '/tmp/zbugs.cpuprofile';

let stopped = false;
async function stopAndExit(code: number) {
  if (stopped) return;
  stopped = true;
  const {profile} = await session.post('Profiler.stop');
  fs.writeFileSync(outPath, JSON.stringify(profile));
  // eslint-disable-next-line no-console
  console.error(`[profile-runner] wrote ${outPath}`);
  process.exit(code);
}

setTimeout(() => void stopAndExit(0), durationMs);

try {
  await import('./zbugs-profile.ts');
  await stopAndExit(0);
} catch (e) {
  // eslint-disable-next-line no-console
  console.error('[profile-runner] script threw:', e);
  await stopAndExit(1);
}
