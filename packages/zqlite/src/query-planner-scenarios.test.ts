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
  const knownFailure = scenario.knownFailure;

  if (knownFailure) {
    test(`${scenario.name} current SQL`, () => {
      const result = runQueryScenario(scenario);

      expect(result.sql).toEqual(knownFailure.currentSQL);
    });

    test.fails(`${scenario.name} desired optimization`, () => {
      assertScenarioExpectations(scenario);
    });
    continue;
  }

  test(scenario.name, () => {
    assertScenarioExpectations(scenario);
  });
}

function assertScenarioExpectations(
  scenario: QueryScenario<typeof educationAppSchema>,
) {
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
}
