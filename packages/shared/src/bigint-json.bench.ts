import {bench, describe, use} from './bench.ts';
import {parse} from './bigint-json.ts';

function row(i: number, id: string | bigint = `issue-${i}`) {
  return {
    id,
    title: `Issue number ${i} with a reasonably long title`,
    open: i % 2 === 0,
    priority: i % 5,
    created: 1729317600000 + i,
    modified: 1729317600000 + i * 1000,
    creatorID: `user-${i % 100}`,
    description: null,
    labels: ['bug', 'p1'],
  };
}

function json(value: unknown) {
  return JSON.stringify(value, (_, v) =>
    typeof v === 'bigint' ? `__big${v}__` : v,
  ).replace(/"__big(\d+)__"/g, '$1');
}

const cases = {
  'row key': '{"id":"issue-123"}',
  'row key (bigint)': '{"id":9007199254740993}',
  'row': json(row(1)),
  'row (bigint id)': json(row(1, 9007199254740993n)),
  '100 rows': json(Array.from({length: 100}, (_, i) => row(i))),
  '100 rows (last bigint)': json(
    Array.from({length: 100}, (_, i) =>
      row(i, i === 99 ? 9007199254740993n : `issue-${i}`),
    ),
  ),
};

describe('bigint-json parse', () => {
  for (const [name, str] of Object.entries(cases)) {
    bench(name, () => {
      use(parse(str));
    });
  }
});
