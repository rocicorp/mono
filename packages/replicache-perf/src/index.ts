import * as m from './perf.js';
import {benchmarks, runAll} from './perf.js';

// export all as globals
for (const [n, v] of Object.entries(m)) {
  (globalThis as Record<string, unknown>)[n] = v;
}

const selected = window.location.search
  .slice(1)
  .split('&')
  .map(kv => kv.split('='))
  .filter(([k]) => k === 'group')
  .map(([, v]) => v);

window.onload = () => {
  const form = document.querySelector<HTMLFormElement>('#group-form');
  if (!form) {
    throw new Error('no form');
  }

  [...new Set(benchmarks.map(b => b.group))].forEach(group => {
    const label = document.createElement('label');
    const input = document.createElement('input');
    input.type = 'checkbox';
    input.name = 'group';
    input.value = group;

    input.checked = selected.indexOf(group) > -1;
    input.onchange = () => form.submit();
    label.appendChild(input);
    label.appendChild(document.createTextNode(group));
    form.appendChild(label);

    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    document.querySelector('button')!.onclick = () => runAll(selected);
  });
};
