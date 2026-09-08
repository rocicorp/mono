import {StatusBar} from 'expo-status-bar';
import {useCallback, useEffect, useRef, useState} from 'react';
import {
  DevSettings,
  ScrollView,
  StyleSheet,
  Text,
  TouchableOpacity,
  View,
} from 'react-native';
import {
  backends,
  configure,
  findBenchmarks,
  formatAsReplicache,
  runBenchmarkByNameAndGroup,
  setBackend,
  type Backend,
  type BenchmarkResult,
} from './benchmarks.js';

/**
 * Set by the runner (`perf-rn.ts`) when it spawns Metro. Absent when the app is
 * started by hand, which puts it in manual mode.
 *
 * `localhost` reaches the host on the iOS simulator directly and on Android via
 * the runner's `adb reverse`.
 */
// In a release build the JS bundle is produced by gradle/Xcode, which does not
// inherit the runner's environment, so EXPO_PUBLIC_PERF_PORT is not inlined —
// fall back to the runner's default port.
const port = process.env.EXPO_PUBLIC_PERF_PORT ?? '9099';
const base = `http://localhost:${port}`;

// Exposed so a CDP client (see perf-rn.ts --profile) can drive a single
// benchmark via Runtime.evaluate and bracket a Hermes trace around exactly
// that call, with no control-server round trips inside the measured region.
(globalThis as Record<string, unknown>).__replicachePerf = {
  configure,
  findBenchmarks,
  formatAsReplicache,
  runBenchmarkByNameAndGroup,
  setBackend,
};

type Next =
  | {done: true}
  | {
      done: false;
      backend: Backend;
      name: string;
      group: string;
      tmcwUrl: string;
      index: number;
      total: number;
    };

type Status = 'connecting' | 'running' | 'finished' | 'manual';

export default function App() {
  const [status, setStatus] = useState<Status>(base ? 'connecting' : 'manual');
  const [detail, setDetail] = useState('');
  const [lines, setLines] = useState<string[]>([]);
  const [backend, setBackendState] = useState<Backend>('expo');
  const started = useRef(false);

  const log = useCallback((line: string) => {
    // Also to the console so the Metro terminal is readable.
    // oxlint-disable-next-line no-console
    console.log(line);
    setLines(prev => [...prev, line]);
  }, []);

  /**
   * Driven mode: pull one benchmark, run it, post the result, then reload so
   * the next one gets a fresh JS context — the analogue of `page.reload()` in
   * the browser runner.
   */
  const drive = useCallback(async () => {
    if (!base) {
      return;
    }
    // Release builds have no dev-settings reload, so there we stay in one JS
    // context and loop instead. Less isolation between benchmarks than the
    // reload gives, but teardownEach still cleans up each rep.
    const canReload = __DEV__ && typeof DevSettings?.reload === 'function';

    for (;;) {
      let next: Next;
      try {
        next = (await fetch(`${base}/next`).then(r => r.json())) as Next;
      } catch {
        // Nobody is driving us; fall back to the manual UI.
        setStatus('manual');
        return;
      }

      if (next.done) {
        setStatus('finished');
        setDetail('All benchmarks reported.');
        return;
      }

      setStatus('running');
      setDetail(
        `[${next.index + 1}/${next.total}] ${next.backend} · ${next.name}`,
      );
      configure({backend: next.backend, tmcwUrl: next.tmcwUrl});

      let body: {result: BenchmarkResult} | {error: string};
      try {
        const out = await runBenchmarkByNameAndGroup(next.name, next.group);
        if (out && out[0] === 'result') {
          body = {result: out[1]};
          log(formatAsReplicache(out[1]));
        } else {
          body = {error: String(out?.[1] ?? 'no result')};
          log(`${next.name}: ${(body as {error: string}).error}`);
        }
      } catch (e) {
        body = {error: (e as Error)?.stack ?? String(e)};
        log(`${next.name} THREW: ${body.error}`);
      }

      await fetch(`${base}/result`, {
        method: 'POST',
        headers: {'content-type': 'application/json'},
        body: JSON.stringify(body),
      });

      if (canReload) {
        DevSettings.reload();
        return;
      }
    }
  }, [log]);

  useEffect(() => {
    if (started.current) {
      return;
    }
    started.current = true;
    void drive();
  }, [drive]);

  /** Manual mode: run the whole group in one context, no server needed. */
  const runManually = useCallback(async () => {
    setStatus('running');
    setLines([]);
    setBackend(backend);
    const bs = findBenchmarks(['replicache'], []);
    for (const [i, b] of bs.entries()) {
      setDetail(`[${i + 1}/${bs.length}] ${backend} · ${b.name}`);
      try {
        const out = await runBenchmarkByNameAndGroup(b.name, b.group);
        log(
          out && out[0] === 'result'
            ? formatAsReplicache(out[1])
            : `${b.name}: ${String(out?.[1] ?? 'no result')}`,
        );
      } catch (e) {
        log(`${b.name} THREW: ${(e as Error)?.message ?? String(e)}`);
      }
    }
    setStatus('finished');
    setDetail(`${bs.length} benchmarks done`);
  }, [backend, log]);

  return (
    <View style={styles.container}>
      <StatusBar style="auto" />
      <Text style={styles.title}>replicache-perf</Text>
      <Text style={styles.status}>
        {status === 'connecting' && `connecting to ${base}…`}
        {status === 'running' && '⏱  running'}
        {status === 'finished' && '✅ done'}
        {status === 'manual' && 'manual mode — no control server'}
      </Text>
      {detail !== '' && <Text style={styles.detail}>{detail}</Text>}

      {status === 'manual' && (
        <>
          <View style={styles.row}>
            {backends.map(b => (
              <TouchableOpacity
                key={b}
                style={[styles.chip, b === backend && styles.chipOn]}
                onPress={() => setBackendState(b)}
              >
                <Text
                  style={[styles.chipText, b === backend && styles.chipTextOn]}
                >
                  {b}
                </Text>
              </TouchableOpacity>
            ))}
          </View>
          <TouchableOpacity style={styles.button} onPress={runManually}>
            <Text style={styles.buttonText}>Run replicache group</Text>
          </TouchableOpacity>
        </>
      )}

      <ScrollView style={styles.log}>
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
  container: {
    flex: 1,
    backgroundColor: '#fff',
    paddingTop: 72,
    paddingHorizontal: 20,
  },
  title: {fontSize: 22, fontWeight: '700'},
  status: {fontSize: 15, marginTop: 6, color: '#334155'},
  detail: {fontSize: 12, marginTop: 6, color: '#64748b'},
  row: {flexDirection: 'row', marginTop: 16, gap: 8},
  chip: {
    paddingVertical: 8,
    paddingHorizontal: 16,
    borderRadius: 999,
    borderWidth: 1,
    borderColor: '#cbd5e1',
  },
  chipOn: {backgroundColor: '#2563eb', borderColor: '#2563eb'},
  chipText: {color: '#334155', fontWeight: '600'},
  chipTextOn: {color: '#fff'},
  button: {
    marginTop: 12,
    backgroundColor: '#2563eb',
    paddingVertical: 14,
    borderRadius: 10,
    alignItems: 'center',
  },
  buttonText: {color: '#fff', fontSize: 16, fontWeight: '600'},
  log: {marginTop: 16, flex: 1},
  line: {
    fontFamily: 'Courier',
    fontSize: 11,
    marginVertical: 2,
    color: '#0f172a',
  },
});
