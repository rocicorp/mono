// oxlint-disable no-console
import {afterAll, beforeAll, describe, expect, test} from 'vitest';
import {
  queries,
  initializePlannerInfrastructure,
  executeAllPlanAttempts,
  validateCorrelation,
  validateWithinOptimal,
  validateWithinBaseline,
  printTestSummary,
  type ValidationResult,
  type TestSummary,
} from './planner-exec-helpers.ts';

// queries is used in test cases added to the test.each array below
void queries;

const testSummaries: TestSummary[] = [];

describe('Tera planner execution cost validation', () => {
  beforeAll(() => {
    initializePlannerInfrastructure();
  }, 120000);

  afterAll(() => {
    printTestSummary(testSummaries, {
      title: 'TERA',
      includeIndexed: false,
      includeImpactSummary: false,
    });
  });

  test('placeholder - add test cases to the test.each array below', () => {
    // This test exists to prevent vitest from failing with "No test found in suite"
    // when the test.each array is empty. Remove this once real test cases are added.
    expect(true).toBe(true);
  });

  // Add test cases here following this pattern:
  // {
  //   name: 'descriptive test name',
  //   query: queries.issue.where(...).whereExists(...).limit(N),
  //   validations: [
  //     ['correlation', 0.8],
  //     ['within-optimal', 1.5],
  //     ['within-baseline', 1],
  //   ],
  // },
  test.each(
    [] as {
      name: string;
      // oxlint-disable-next-line @typescript-eslint/no-explicit-any
      query: any;
      validations: [string, number][];
    }[],
  )(
    '$name',
    ({name, query, validations}) => {
      const results = executeAllPlanAttempts(query, 100_000);

      expect(results.length).toBeGreaterThan(0);

      const summary: TestSummary = {
        name,
        base: {},
        indexed: {},
      };

      const validationResults: ValidationResult[] = [];

      for (const validation of validations) {
        const [validationType, threshold] = validation;

        if (validationType === 'correlation') {
          const result = validateCorrelation(results, threshold);
          validationResults.push(result);
          summary.base.correlation = result.actualValue;
          summary.base.correlationThreshold = threshold;
        } else if (validationType === 'within-optimal') {
          const result = validateWithinOptimal(results, threshold);
          validationResults.push(result);
          summary.base.withinOptimal = result.actualValue;
          summary.base.withinOptimalThreshold = threshold;
        } else if (validationType === 'within-baseline') {
          const result = validateWithinBaseline(results, threshold);
          validationResults.push(result);
          summary.base.withinBaseline = result.actualValue;
          summary.base.withinBaselineThreshold = threshold;
        }
      }

      const failedValidations = validationResults.filter(v => !v.passed);

      testSummaries.push(summary);

      if (failedValidations.length > 0) {
        const estimatedCosts = results.map(r => r.estimatedCost);
        const actualCosts = results.map(r => r.actualRowsScanned);

        console.log('\n=== FAILED VALIDATIONS ===');
        for (const v of failedValidations) {
          console.log(`[${v.type}] ${v.details}`);
        }
        console.log('\nEstimated costs:', estimatedCosts);
        console.log('Actual costs:', actualCosts);
        console.log('\nDetailed results:');
        for (const r of results) {
          console.log(
            `  Attempt ${r.attemptNumber}: est=${r.estimatedCost}, actual=${r.actualRowsScanned}, flip=${r.flipPattern}`,
          );
        }
      }

      expect(failedValidations).toHaveLength(0);
    },
    120000,
  );
});
