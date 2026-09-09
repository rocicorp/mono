import {useEffect, useRef, useState} from 'react';
import {DevSettings, ScrollView, StyleSheet, Text, View} from 'react-native';
import {
  configure,
  formatAsReplicache,
  runBenchmarkByNameAndGroup,
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
(globalThis as Record<string, unknown>).__zqlPerf = {
  configure,
  formatAsReplicache,
  runBenchmarkByNameAndGroup,
};

type Next =
  | {done: true}
  | {
      done: false;
      name: string;
      group: string;
      index: number;
      total: number;
    };

export default function App() {
  const [status, setStatus] = useState('connecting…');
  const [lines, setLines] = useState<string[]>([]);
  const started = useRef(false);

  useEffect(() => {
    if (started.current) {
      return;
    }
    started.current = true;

    const log = (line: string) => {
      // Also to the console so the Metro terminal is readable.
      // oxlint-disable-next-line no-console
      console.log(line);
      setLines(prev => [...prev, line]);
    };

    void (async () => {
      // Release builds have no dev-settings reload, so there we stay in one JS
      // context and loop. Less isolation between benchmarks than a reload
      // gives, but each benchmark rebuilds its own sources anyway.
      const canReload = __DEV__ && typeof DevSettings?.reload === 'function';

      for (;;) {
        let next: Next;
        try {
          next = (await fetch(`${base}/next`).then(r => r.json())) as Next;
        } catch {
          setStatus(`no control server on ${base}`);
          return;
        }
        if (next.done) {
          setStatus('done');
          return;
        }

        setStatus(
          `[${next.index + 1}/${next.total}] ${next.group} · ${next.name}`,
        );

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
    })();
  }, []);

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
