import {expect, test} from 'vitest';
import {
  runQueryScenario,
  type QueryScenario,
  type QueryScenarioExpectations,
} from './test/query-scenario.ts';
import type {educationAppSchema} from './test/query-scenarios/education-app.ts';
import scenarios from './test/query-scenarios/scenarios/index.ts';

for (const scenario of scenarios as readonly QueryScenario<
  typeof educationAppSchema
>[]) {
  const scenarioTest = scenario.knownFailure ? test.fails : test;

  scenarioTest(scenario.name, () => {
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
}
