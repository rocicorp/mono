import {
  createPlannerInfrastructure,
  validateCorrelation,
  validateWithinOptimal,
  validateWithinBaseline,
  printTestSummary,
  type PlanAttemptResult,
  type PlanValidation,
  type ValidationResult,
  type TestSummary,
} from '../helpers/planner-exec.ts';
import {getPagila} from './get-deps.ts';
import {schema} from './schema.ts';

// Pagila-specific indices for the indexed database
const PAGILA_INDICES = [
  // Film and content
  'CREATE INDEX IF NOT EXISTS idx_film_title ON film(title)',
  'CREATE INDEX IF NOT EXISTS idx_film_rating ON film(rating)',
  'CREATE INDEX IF NOT EXISTS idx_film_release_year ON film(release_year)',
  'CREATE INDEX IF NOT EXISTS idx_actor_last_name ON actor(last_name)',
  'CREATE INDEX IF NOT EXISTS idx_category_name ON category(name)',
  // Geographic
  'CREATE INDEX IF NOT EXISTS idx_country_country ON country(country)',
  'CREATE INDEX IF NOT EXISTS idx_city_city ON city(city)',
  'CREATE INDEX IF NOT EXISTS idx_address_district ON address(district)',
  // Customer and staff
  'CREATE INDEX IF NOT EXISTS idx_customer_last_name ON customer(last_name)',
  'CREATE INDEX IF NOT EXISTS idx_customer_email ON customer(email)',
  'CREATE INDEX IF NOT EXISTS idx_staff_last_name ON staff(last_name)',
  // Transactions
  'CREATE INDEX IF NOT EXISTS idx_rental_rental_date ON rental(rental_date)',
  'CREATE INDEX IF NOT EXISTS idx_rental_return_date ON rental(return_date)',
  'CREATE INDEX IF NOT EXISTS idx_payment_amount ON payment(amount)',
  'CREATE INDEX IF NOT EXISTS idx_payment_payment_date ON payment(payment_date)',
];

// Load Pagila data
export const pgContent = await getPagila();

// Create infrastructure using shared helper
const infra = await createPlannerInfrastructure({
  suiteName: 'pagila_planner_exec',
  pgContent,
  schema,
  indices: PAGILA_INDICES,
});

// Re-export infrastructure components
export const {
  dbs,
  queries,
  delegates,
  indexedDb,
  indexedDelegate,
  initializePlannerInfrastructure,
  initializeIndexedDatabase,
  executeAllPlanAttempts,
} = infra;

// Re-export getters for mutable state
export const costModel = infra.costModel;
export const mapper = infra.mapper;
export const tableSpecs = infra.tableSpecs;
export const indexedCostModel = infra.indexedCostModel;
export const indexedTableSpecs = infra.indexedTableSpecs;

// Re-export types and validation functions from shared module
export {
  validateCorrelation,
  validateWithinOptimal,
  validateWithinBaseline,
  printTestSummary,
  type PlanAttemptResult,
  type PlanValidation,
  type ValidationResult,
  type TestSummary,
};
