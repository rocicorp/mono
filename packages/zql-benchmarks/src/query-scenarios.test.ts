import {expect, test} from 'vitest';
import {runQueryScenario} from './query-scenarios/scenario.ts';
import scenarios from './query-scenarios/scenarios/index.ts';

for (const scenario of scenarios) {
  test(scenario.name, () => {
    const result = runQueryScenario(scenario);
    expect(result.sql).toMatchSnapshot('sql');
    expect(result.rows).toMatchSnapshot('rows');
  });
}
