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
import {getChinook} from './get-deps.ts';
import {schema} from './schema.ts';

// Chinook-specific indices for the indexed database
const CHINOOK_INDICES = [
  'CREATE INDEX IF NOT EXISTS idx_album_title ON album(title)',
  'CREATE INDEX IF NOT EXISTS idx_artist_name ON artist(name)',
  'CREATE INDEX IF NOT EXISTS idx_track_composer ON track(composer)',
  'CREATE INDEX IF NOT EXISTS idx_track_milliseconds ON track(milliseconds)',
  'CREATE INDEX IF NOT EXISTS idx_track_name ON track(name)',
  'CREATE INDEX IF NOT EXISTS idx_genre_name ON genre(name)',
  'CREATE INDEX IF NOT EXISTS idx_employee_title ON employee(title)',
  'CREATE INDEX IF NOT EXISTS idx_customer_country ON customer(country)',
  'CREATE INDEX IF NOT EXISTS idx_playlist_name ON playlist(name)',
  'CREATE INDEX IF NOT EXISTS idx_invoice_line_quantity ON invoice_line(quantity)',
];

// Load Chinook data
export const pgContent = await getChinook();

// Create infrastructure using shared helper
const infra = await createPlannerInfrastructure({
  suiteName: 'chinook_planner_exec',
  pgContent,
  schema,
  indices: CHINOOK_INDICES,
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
