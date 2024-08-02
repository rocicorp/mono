import * as m from './perf.js';
import {benchmarks, runAll} from './perf.js';

// export all as globals
for (const [n, v] of Object.entries(m)) {
  (globalThis as Record<string, unknown>)[n] = v;
}

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

    input.checked = selected.indexOf(group) > -1;
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

    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    document.querySelector('button')!.onclick = () => runAll(selected, runs);
  });
};
