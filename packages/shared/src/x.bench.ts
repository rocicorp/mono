import {bench, describe, use} from './bench.ts';

describe('string collation benchmarks', () => {
  bench('String.prototype.localeCompare', () => {
    use('foo'.localeCompare('bar'));
  });

  bench('Intl.Collator', function* () {
    const collator = new Intl.Collator();
    const compare = collator.compare.bind(collator);

    yield () => {
      use(compare('foo', 'bar'));
    };
  });
});
