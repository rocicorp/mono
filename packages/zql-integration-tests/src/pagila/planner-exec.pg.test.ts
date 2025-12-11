import {afterAll, beforeAll, describe, expect, test} from 'vitest';
import {
  queries,
  initializePlannerInfrastructure,
  initializeIndexedDatabase,
  executeAllPlanAttempts,
  validateCorrelation,
  validateWithinOptimal,
  validateWithinBaseline,
  printTestSummary,
  type ValidationResult,
  type TestSummary,
} from './planner-exec-helpers.ts';

const testSummaries: TestSummary[] = [];

describe('Pagila planner execution cost validation', () => {
  beforeAll(() => {
    initializePlannerInfrastructure();
    initializeIndexedDatabase();
  }, 60000);

  afterAll(() => {
    printTestSummary(testSummaries, {
      title: 'PAGILA',
      includeIndexed: false,
      includeImpactSummary: false,
    });
  });

  test.each([
    // ==========================================================================
    // Geographic hierarchy tests (4-level: customer → address → city → country)
    // ==========================================================================
    {
      name: 'geographic chain - customer to country',
      query: queries.customer
        .whereExists('address', a =>
          a.whereExists('city', c =>
            c.whereExists('country', co =>
              co.where('country', 'United States'),
            ),
          ),
        )
        .limit(10),
      validations: [
        ['correlation', 0.85],
        ['within-optimal', 1.7],
        ['within-baseline', 1],
      ],
    },

    {
      name: 'geographic chain - address to country with filter',
      query: queries.address
        .where('district', 'California')
        .whereExists('city', c => c.whereExists('country')),
      validations: [
        ['correlation', 1],
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
    },

    // ==========================================================================
    // Film/actor junction tests (many-to-many)
    // ==========================================================================
    {
      name: 'film via actor - single actor filter',
      query: queries.film.whereExists('actors', a =>
        a.where('lastName', 'GUINESS'),
      ),
      validations: [
        ['correlation', 0.4],
        ['within-optimal', 1],
        ['within-baseline', 0.013],
      ],
    },

    {
      name: 'actor via film - specific film title',
      query: queries.actor.whereExists('films', f =>
        f.where('title', 'ACADEMY DINOSAUR'),
      ),
      validations: [
        ['correlation', 0.8],
        ['within-optimal', 1],
        ['within-baseline', 0.005],
      ],
    },

    {
      name: 'film with actor and category filters',
      query: queries.film
        .whereExists('actors', a => a.where('lastName', 'GUINESS'))
        .whereExists('categories', c => c.where('name', 'Action')),
      validations: [
        ['correlation', -0.08],
        ['within-optimal', 1],
        ['within-baseline', 0.04],
      ],
    },

    // ==========================================================================
    // Rental chain tests (rental → inventory → film)
    // ==========================================================================
    {
      name: 'rental to film via inventory',
      query: queries.rental.whereExists('inventory', i =>
        i.whereExists('film', f => f.where('title', 'ACADEMY DINOSAUR')),
      ),
      validations: [
        ['correlation', 0.94],
        ['within-optimal', 1],
        ['within-baseline', 0.003],
      ],
    },

    {
      name: 'rental with customer and film filters',
      query: queries.rental
        .whereExists('customer', c => c.where('lastName', 'SMITH'))
        .whereExists('inventory', i =>
          i.whereExists('film', f => f.where('rating', 'PG')),
        ),
      validations: [
        ['correlation', 0.4],
        ['within-optimal', 2.1],
        ['within-baseline', 1],
      ],
    },

    // ==========================================================================
    // Payment chain tests (payment → rental → inventory → film)
    // ==========================================================================
    {
      name: 'payment to film via rental chain (3 hops)',
      query: queries.payment
        .whereExists('rental', r =>
          r.whereExists('inventory', i =>
            i.whereExists('film', f => f.where('title', 'ACADEMY DINOSAUR')),
          ),
        )
        .limit(100),
      validations: [
        ['correlation', -0.5],
        // Correlation is bad for this one and we chose a non-optimal query.
        // What the below is saying is that the query as written is most optimal
        // (within-optimal and within-baseline are the same).
        // On inspection of the data, however, the chosen plan (everything flipped) should be close to optimal.
        // The issue is that walking back up the tree (from most nested exists check) is missing indices.
        //
        // 1. payment table has NO index on rental_id
        // 2. inventory table has NO index on film_id
        // We need to come up with a good way to penalize `SCAN` vs `SEARCH`
        ['within-optimal', 10],
        ['within-baseline', 10],
      ],
    },

    {
      name: 'high-value payments with film filter',
      query: queries.payment
        .where('amount', '>', 5)
        .whereExists('rental', r =>
          r.whereExists('inventory', i => i.whereExists('film')),
        )
        .limit(20),
      validations: [
        ['correlation', 1],
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
    },

    // ==========================================================================
    // Store/staff hierarchy tests
    // ==========================================================================
    {
      name: 'staff with store address',
      query: queries.staff.whereExists('store', s =>
        s.whereExists('address', a =>
          a.whereExists('city', c => c.where('city', 'Lethbridge')),
        ),
      ),
      validations: [
        ['correlation', 0],
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
    },

    {
      name: 'customer with store filter',
      query: queries.customer
        .whereExists('store', s =>
          s.whereExists('address', a => a.whereExists('city')),
        )
        .limit(100),
      validations: [
        ['correlation', 0],
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
    },

    // ==========================================================================
    // Fanout tests
    // ==========================================================================
    {
      name: 'high fanout - film to all actors',
      // interesting test of pk lookup w/ existence lookup
      query: queries.film.where('id', 1).whereExists('actors'),
      validations: [
        ['correlation', 0.8],
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
    },

    {
      name: 'high fanout - store inventory',
      query: queries.store.where('id', 1).whereExists('inventory'),
      validations: [
        ['correlation', -1],
        ['within-optimal', 1.4],
        ['within-baseline', 1],
      ],
    },

    // ==========================================================================
    // Limit tests
    // ==========================================================================
    {
      name: 'limit 5 with deep join',
      query: queries.rental
        .whereExists('inventory', i =>
          i.whereExists('film', f => f.where('rating', 'PG-13')),
        )
        .limit(5),
      validations: [
        ['correlation', 0.4],
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
    },

    {
      name: 'limit 50 with geographic filter',
      query: queries.customer
        .whereExists('address', a =>
          a.whereExists('city', c =>
            c.whereExists('country', co => co.where('country', 'Canada')),
          ),
        )
        .limit(50),
      validations: [
        ['correlation', 0.77],
        ['within-optimal', 1],
        ['within-baseline', 0.08],
      ],
    },

    // ==========================================================================
    // Empty/sparse result tests
    // ==========================================================================
    {
      name: 'empty result - nonexistent actor',
      query: queries.film.whereExists('actors', a =>
        a.where('lastName', 'NONEXISTENT_ACTOR_ZZZZZ'),
      ),
      // within-optimal excluded: empty results cause divide-by-zero (optimal has 0 rows)
      validations: [
        ['correlation', 0.8],
        ['within-baseline', 0.01],
      ],
    },

    {
      name: 'sparse result - rare rating filter',
      query: queries.film
        .where('rating', 'NC-17')
        .whereExists('actors')
        .limit(10),
      validations: [
        ['correlation', 0.8],
        ['within-optimal', 1],
        ['within-baseline', 1],
      ],
    },

    // ==========================================================================
    // Complex multi-path tests
    // ==========================================================================
    {
      name: 'film with language and actors',
      query: queries.film
        .whereExists('language', l => l.where('name', 'English'))
        .whereExists('actors', a => a.where('lastName', 'BERRY')),
      validations: [
        ['correlation', 0.77],
        ['within-optimal', 31],
        ['within-baseline', 0.2],
      ],
    },

    {
      name: 'customer with address and rentals',
      query: queries.customer
        .whereExists('address', a => a.where('district', 'Alberta'))
        .whereExists('rentals'),
      validations: [
        ['correlation', 1],
        ['within-optimal', 1],
        ['within-baseline', 0.51],
      ],
    },
  ])(
    '$name',
    ({name, query, validations}) => {
      // if (name !== 'payment to film via rental chain (3 hops)') {
      //   return;
      // }
      // Execute all plan attempts and collect results (baseline DB)
      const results = executeAllPlanAttempts(query, false, 40_000);

      // Verify we got multiple planning attempts
      expect(results.length).toBeGreaterThan(0);

      // Initialize summary entry
      const summary: TestSummary = {
        name,
        base: {},
        indexed: {},
      };

      // Run requested validations
      const validationResults: ValidationResult[] = [];

      for (const validation of validations) {
        const [validationType, threshold] = validation as [string, number];

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

      // Check if all validations passed
      let failedValidations = validationResults.filter(v => !v.passed);

      // Store summary for final report
      testSummaries.push(summary);

      if (failedValidations.length > 0) {
        const estimatedCosts = results.map(r => r.estimatedCost);
        const actualCosts = results.map(r => r.actualRowsScanned);

        // eslint-disable-next-line no-console
        console.log('\n=== FAILED VALIDATIONS ===');
        for (const v of failedValidations) {
          // eslint-disable-next-line no-console
          console.log(`[${v.type}] ${v.details}`);
        }
        // eslint-disable-next-line no-console
        console.log('\nEstimated costs:', estimatedCosts);
        // eslint-disable-next-line no-console
        console.log('Actual costs:', actualCosts);
        // eslint-disable-next-line no-console
        console.log('\nDetailed results:');
        for (const r of results) {
          // eslint-disable-next-line no-console
          console.log(
            `  Attempt ${r.attemptNumber}: est=${r.estimatedCost}, actual=${r.actualRowsScanned}, flip=${r.flipPattern}`,
          );
        }
      }

      // Assert all validations passed
      expect(failedValidations).toHaveLength(0);
    },
    60000,
  );
});
