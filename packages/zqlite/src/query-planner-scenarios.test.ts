import {expect, test} from 'vitest';
import {
  runQueryScenario,
  type QueryScenarioExpectations,
} from './test/query-scenario.ts';
import scenarios from './test/query-scenarios/scenarios/index.ts';

test.each(scenarios)('$name', scenario => {
  const result = runQueryScenario(scenario);
  const expectations: QueryScenarioExpectations = scenario.expectations;

  if (expectations.optimizedAST) {
    expect(result.optimizedAST).toMatchObject(expectations.optimizedAST);
  }
  for (const planDebug of expectations.planDebug ?? []) {
    expect(result.planDebug).toContain(planDebug);
  }
  if (expectations.sql) {
    expect(result.sql).toEqual(expectations.sql);
  }
});
