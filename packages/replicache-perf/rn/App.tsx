import {useEffect, useSyncExternalStore} from 'react';
import {DevSettings, ScrollView, StyleSheet, Text, View} from 'react-native';
import {
  configure,
  formatAsReplicache,
  runBenchmarkByNameAndGroup,
  type Backend,
  type BenchmarkResult,
} from './benchmarks.js';

/**
 * Set by the runner (perf-rn.ts) when it spawns Metro. A release bundle is
 * built by gradle/Xcode, which do not inherit the runner's environment, so it
 * is absent there and we fall back to the runner's default port.
 *
 * `localhost` reaches the host directly on the iOS simulator, and on Android
 * via the runner's `adb reverse`.
 */
const base = `http://localhost:${process.env.EXPO_PUBLIC_PERF_PORT ?? '9099'}`;

/**
 * Exposed so a CDP client can drive one benchmark via Runtime.evaluate and
 * bracket a Hermes trace around exactly that call. See perf-rn.ts --profile.
 */
(globalThis as Record<string, unknown>).__replicachePerf = {
  configure,
  formatAsReplicache,
  runBenchmarkByNameAndGroup,
};

type Next =
  | {done: true}
  | {
      done: false;
      backend: Backend;
      variant: string;
      name: string;
      group: string;
      tmcwUrl: string;
      index: number;
      total: number;
    };

type State = {status: string; lines: string[]};

/**
 * Run state lives at module scope, not in the component. Android can re-create
 * the Activity while the process (and so this JS context) stays alive, which
 * mounts App a second time. A per-instance guard would then start a second
 * loop that runs the same benchmark concurrently, sharing its stores, and
 * posts a duplicate result. A remounted App just subscribes to the one loop.
 */
let state: State = {status: 'connecting…', lines: []};
const listeners = new Set<() => void>();
let running = false;

function update(patch: Partial<State>) {
  state = {...state, ...patch};
  for (const l of listeners) {
    l();
  }
}

function subscribe(l: () => void) {
  listeners.add(l);
  return () => {
    listeners.delete(l);
  };
}

function log(line: string) {
  // Also to the console so the Metro terminal is readable.
  // oxlint-disable-next-line no-console
  console.log(line);
  update({lines: [...state.lines, line]});
}

/** Starts the run loop unless one is already running in this JS context. */
function startLoop() {
  if (running) {
    return;
  }
  running = true;
  void runLoop().finally(() => {
    running = false;
  });
}

async function runLoop() {
  // Release builds have no dev-settings reload, so there we stay in one JS
  // context and loop. Less isolation between benchmarks than a reload
  // gives, but teardownEach still cleans up each rep.
  const canReload = __DEV__ && typeof DevSettings?.reload === 'function';

  for (;;) {
    let next: Next;
    try {
      next = (await fetch(`${base}/next`).then(r => r.json())) as Next;
    } catch {
      update({status: `no control server on ${base}`});
      return;
    }
    if (next.done) {
      update({status: 'done'});
      return;
    }

    update({
      status: `[${next.index + 1}/${next.total}] ${next.backend} · ${next.name}`,
    });
    configure({backend: next.backend, tmcwUrl: next.tmcwUrl});

    // Echoed back so the control server can reject a result for a benchmark
    // other than the one in flight instead of attributing it positionally.
    const ran = {
      index: next.index,
      name: next.name,
      group: next.group,
      variant: next.variant,
    };
    let body: {result: BenchmarkResult} | {error: string};
    try {
      const out = await runBenchmarkByNameAndGroup(next.name, next.group);
      if (out && out[0] === 'result') {
        body = {result: out[1]};
        log(formatAsReplicache(out[1]));
      } else {
        body = {error: String(out?.[1] ?? 'no result')};
        log(`${next.name}: ${body.error}`);
      }
    } catch (e) {
      body = {error: (e as Error)?.stack ?? String(e)};
      log(`${next.name} THREW: ${body.error}`);
    }

    const res = await fetch(`${base}/result`, {
      method: 'POST',
      headers: {'content-type': 'application/json'},
      body: JSON.stringify({ran, ...body}),
    });
    if (res.status === 409) {
      // Some other loop owns the queue (this one is stale), so stop rather
      // than keep running benchmarks alongside it.
      log(`result rejected: ${await res.text()}`);
      update({status: 'stopped: result rejected'});
      return;
    }

    if (canReload) {
      DevSettings.reload();
      return;
    }
  }
}

export default function App() {
  const {status, lines} = useSyncExternalStore(subscribe, () => state);

  useEffect(startLoop, []);

  return (
    <View style={styles.container}>
      <Text style={styles.status}>{status}</Text>
      <ScrollView>
        {lines.map((l, i) => (
          <Text key={i} style={styles.line}>
            {l}
          </Text>
        ))}
      </ScrollView>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {flex: 1, paddingTop: 64, paddingHorizontal: 16},
  status: {fontSize: 14, marginBottom: 12},
  line: {fontFamily: 'Courier', fontSize: 11, marginVertical: 2},
});
