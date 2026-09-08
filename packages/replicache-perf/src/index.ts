import {runBenchmark} from './benchmark.ts';
import {formatAsReplicache} from './format.ts';
import * as m from './perf.ts';
import {benchmarks, findBenchmarks} from './perf.ts';

// Lives here rather than in perf.ts so that perf.ts stays DOM-free and can be
// reused by the React Native entry point (rn.ts).
async function runAll(groups: string[], runs: string[]): Promise<void> {
  const out: HTMLElement | null = document.getElementById('out');
  if (!out) {
    return;
  }
  for (const b of findBenchmarks(groups, runs)) {
    try {
      const result = await runBenchmark(b);
      if (result) {
        out.textContent += formatAsReplicache(result) + '\n';
      }
    } catch (e) {
      out.textContent += `${b.name} had an error: ${e}\n`;
    }
  }
  out.textContent += 'Done!\n';
}

// export all as globals
for (const [n, v] of Object.entries(m)) {
  (globalThis as Record<string, unknown>)[n] = v;
}
// runAll used to be exported from perf.ts and hoisted by the loop above; keep
// it on the global for `--devtools` users.
(globalThis as Record<string, unknown>).runAll = runAll;

const {searchParams} = new URL(location.href);
const selected = searchParams.getAll('group');
const runs = searchParams.getAll('run');

window.onload = () => {
  const form = document.querySelector<HTMLFormElement>('#group-form');
  if (!form) {
    throw new Error('no form');
  }

  [...new Set(benchmarks.map(b => b.group))].forEach(group => {
    const label = document.createElement('label');
    label.style.margin = '1ex';

    const input = document.createElement('input');
    input.type = 'checkbox';
    input.name = 'group';
    input.value = group;

    input.checked = selected.includes(group);
    input.onchange = () => {
      const url = new URL(location.href);
      if (input.checked) {
        url.searchParams.append('group', group);
      } else {
        url.searchParams.delete('group');
        selected.splice(selected.indexOf(group), 1);
        for (const group of selected) {
          url.searchParams.append('group', group);
        }
      }
      location.replace(url.toString());
    };
    label.append(input, ' ', group);
    form.append(label);

    // oxlint-disable-next-line @typescript-eslint/no-non-null-assertion
    document.querySelector('button')!.onclick = () => runAll(selected, runs);
  });
};
