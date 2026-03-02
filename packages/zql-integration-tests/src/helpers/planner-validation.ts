// oxlint-disable no-console
import {spearmanCorrelation} from './correlation.ts';

// =============================================================================
// Type definitions
// =============================================================================

export type PlanAttemptResult = {
  attemptNumber: number;
  estimatedCost: number;
  actualRowsScanned: number;
  flipPattern: number;
};

export type PlanValidation =
  | ['correlation', number]
  | ['within-optimal', number]
  | ['within-baseline', number];

export type ValidationResult = {
  type: 'correlation' | 'within-optimal' | 'within-baseline';
  passed: boolean;
  details: string;
  actualValue: number;
  threshold: number;
};

// =============================================================================
// Validation functions (schema-agnostic)
// =============================================================================

/**
 * Validate correlation between estimated costs and actual costs using Spearman correlation
 */
export function validateCorrelation(
  results: PlanAttemptResult[],
  threshold: number,
): ValidationResult {
  const estimatedCosts = results.map(r => r.estimatedCost);
  const actualCosts = results.map(r => r.actualRowsScanned);
  const correlation = spearmanCorrelation(estimatedCosts, actualCosts);
  const passed = correlation >= threshold;

  const details = passed
    ? `Spearman correlation ${correlation.toFixed(3)} >= ${threshold} threshold`
    : `Spearman correlation ${correlation.toFixed(3)} < ${threshold} threshold`;

  return {
    type: 'correlation',
    passed,
    details,
    actualValue: correlation,
    threshold,
  };
}

/**
 * Validate that the picked plan (lowest estimated cost) is within tolerance
 * of the optimal plan (lowest actual rows scanned)
 */
export function validateWithinOptimal(
  results: PlanAttemptResult[],
  toleranceFactor: number,
): ValidationResult {
  // Find the picked plan (lowest estimated cost)
  const pickedPlan = results.reduce((best, current) =>
    current.estimatedCost < best.estimatedCost ? current : best,
  );

  // Find the optimal plan (lowest actual rows scanned)
  const optimalPlan = results.reduce((best, current) =>
    current.actualRowsScanned < best.actualRowsScanned ? current : best,
  );

  // Calculate ratio
  const ratio = pickedPlan.actualRowsScanned / optimalPlan.actualRowsScanned;
  const passed = ratio <= toleranceFactor;

  const details = passed
    ? `Picked plan (attempt ${pickedPlan.attemptNumber}) cost ${pickedPlan.actualRowsScanned} is within ${toleranceFactor}x of optimal (attempt ${optimalPlan.attemptNumber}) cost ${optimalPlan.actualRowsScanned} (ratio: ${ratio.toFixed(2)}x)`
    : `Picked plan (attempt ${pickedPlan.attemptNumber}) cost ${pickedPlan.actualRowsScanned} exceeds ${toleranceFactor}x tolerance of optimal (attempt ${optimalPlan.attemptNumber}) cost ${optimalPlan.actualRowsScanned} (ratio: ${ratio.toFixed(2)}x)`;

  return {
    type: 'within-optimal',
    passed,
    details,
    actualValue: ratio,
    threshold: toleranceFactor,
  };
}

/**
 * Validate that the picked plan (lowest estimated cost) is within tolerance
 * of the baseline query-as-written (attempt 0)
 * Formula: picked <= baseline × toleranceFactor
 * - toleranceFactor < 1.0: picked must be better (e.g., 0.5 = picked must be ≤50% of baseline)
 * - toleranceFactor = 1.0: picked must be as good or better than baseline
 * - toleranceFactor > 1.0: picked can be worse (e.g., 1.5 = picked can be ≤150% of baseline)
 */
export function validateWithinBaseline(
  results: PlanAttemptResult[],
  toleranceFactor: number,
): ValidationResult {
  // Find the baseline plan (attempt 0 - query as written)
  const baselinePlan = results.find(r => r.attemptNumber === 0);
  if (!baselinePlan) {
    throw new Error('Baseline plan (attempt 0) not found in results');
  }

  // Find the picked plan (lowest estimated cost)
  const pickedPlan = results.reduce((best, current) =>
    current.estimatedCost < best.estimatedCost ? current : best,
  );

  // Check if picked plan is within tolerance of baseline
  const maxAllowedCost = baselinePlan.actualRowsScanned * toleranceFactor;
  const passed = pickedPlan.actualRowsScanned <= maxAllowedCost;
  const ratio =
    baselinePlan.actualRowsScanned > 0
      ? pickedPlan.actualRowsScanned / baselinePlan.actualRowsScanned
      : 1;

  const details = passed
    ? `Picked plan (attempt ${pickedPlan.attemptNumber}) cost ${pickedPlan.actualRowsScanned} is within ${toleranceFactor}x of baseline (attempt ${baselinePlan.attemptNumber}) cost ${baselinePlan.actualRowsScanned} (ratio: ${ratio.toFixed(2)}x)`
    : `Picked plan (attempt ${pickedPlan.attemptNumber}) cost ${pickedPlan.actualRowsScanned} exceeds ${toleranceFactor}x tolerance of baseline (attempt ${baselinePlan.attemptNumber}) cost ${baselinePlan.actualRowsScanned} (ratio: ${ratio.toFixed(2)}x)`;

  return {
    type: 'within-baseline',
    passed,
    details,
    actualValue: ratio,
    threshold: toleranceFactor,
  };
}

// =============================================================================
// Test summary types and functions
// =============================================================================

export type TestSummary = {
  name: string;
  base: {
    correlation?: number | undefined;
    correlationThreshold?: number | undefined;
    withinOptimal?: number | undefined;
    withinOptimalThreshold?: number | undefined;
    withinBaseline?: number | undefined;
    withinBaselineThreshold?: number | undefined;
  };
  indexed: {
    correlation?: number | undefined;
    correlationThreshold?: number | undefined;
    withinOptimal?: number | undefined;
    withinOptimalThreshold?: number | undefined;
    withinBaseline?: number | undefined;
    withinBaselineThreshold?: number | undefined;
  };
};

/**
 * Print test summary in markdown format
 */
export function printTestSummary(
  summaries: TestSummary[],
  options: {
    title: string;
    includeIndexed?: boolean | undefined;
    includeImpactSummary?: boolean | undefined;
  },
): void {
  const {title, includeIndexed = false, includeImpactSummary = false} = options;

  // Helper to format number or N/A
  const fmt = (num: number | undefined) =>
    num !== undefined ? num.toFixed(2) : 'N/A';

  // Helper to format actual/threshold and indicate if it can be tightened
  const fmtWithThreshold = (
    actual: number | undefined,
    threshold: number | undefined,
    type: 'correlation' | 'within-optimal' | 'within-baseline',
  ) => {
    if (actual === undefined || threshold === undefined) {
      return fmt(actual);
    }

    const actualStr = actual.toFixed(2);
    const thresholdStr = threshold.toFixed(2);

    // Check if can be tightened (has significant headroom)
    let canTighten = false;
    if (type === 'correlation') {
      // For correlation, actual > threshold is good (headroom > 10%)
      canTighten = actual >= threshold && actual - threshold > 0.1;
    } else {
      // For within-optimal and within-baseline, actual < threshold is good
      // Check if actual is significantly better (>10% headroom)
      canTighten =
        actual <= threshold &&
        threshold > 0 &&
        (threshold - actual) / threshold > 0.1;
    }

    if (canTighten) {
      return `${actualStr} (${thresholdStr}) 🔧`;
    } else if (actualStr !== thresholdStr) {
      return `${actualStr} (${thresholdStr})`;
    } else {
      return actualStr;
    }
  };

  // Print summary table in markdown format
  console.log(`\n\n=== ${title} VALIDATION SUMMARY (Markdown Table) ===\n`);

  // Print markdown table header
  if (includeIndexed) {
    console.log(
      '| Test Name | Base: corr | Base: opt | Base: baseline | Indexed: corr | Indexed: opt | Indexed: baseline |',
    );
    console.log(
      '|-----------|------------|-----------|----------------|---------------|--------------|-------------------|',
    );
  } else {
    console.log('| Test Name | Base: corr | Base: opt | Base: baseline |');
    console.log('|-----------|------------|-----------|----------------|');
  }

  // Print rows
  for (const summary of summaries) {
    let row =
      `| ${summary.name} ` +
      `| ${fmtWithThreshold(summary.base.correlation, summary.base.correlationThreshold, 'correlation')} ` +
      `| ${fmtWithThreshold(summary.base.withinOptimal, summary.base.withinOptimalThreshold, 'within-optimal')} ` +
      `| ${fmtWithThreshold(summary.base.withinBaseline, summary.base.withinBaselineThreshold, 'within-baseline')} `;

    if (includeIndexed) {
      row +=
        `| ${fmtWithThreshold(summary.indexed.correlation, summary.indexed.correlationThreshold, 'correlation')} ` +
        `| ${fmtWithThreshold(summary.indexed.withinOptimal, summary.indexed.withinOptimalThreshold, 'within-optimal')} ` +
        `| ${fmtWithThreshold(summary.indexed.withinBaseline, summary.indexed.withinBaselineThreshold, 'within-baseline')} `;
    }
    row += '|';
    console.log(row);
  }

  console.log('\n🔧 = Can be tightened (>10% headroom)\n');

  // Print impact summary if requested
  if (includeImpactSummary) {
    console.log('\n=== INDEXED DB IMPACT SUMMARY ===\n');
    console.log(
      '| Test Name | Correlation Impact | Within-Optimal Impact | Within-Baseline Impact |',
    );
    console.log(
      '|-----------|--------------------|-----------------------|------------------------|',
    );

    for (const summary of summaries) {
      const corrImpact = (() => {
        if (
          summary.base.correlation === undefined ||
          summary.indexed.correlation === undefined
        ) {
          return 'N/A';
        }
        const delta = summary.indexed.correlation - summary.base.correlation;
        if (Math.abs(delta) < 0.05) return '→ (no change)';
        if (delta > 0) return `↑ +${delta.toFixed(2)} (better)`;
        return `↓ ${delta.toFixed(2)} (worse)`;
      })();

      const optImpact = (() => {
        if (
          summary.base.withinOptimal === undefined ||
          summary.indexed.withinOptimal === undefined
        ) {
          return 'N/A';
        }
        const base = summary.base.withinOptimal;
        const indexed = summary.indexed.withinOptimal;
        const delta = indexed - base;

        if (Math.abs(delta) < 0.05) return '→ (no change)';

        // For within-optimal, lower is better (closer to optimal plan)
        if (delta < 0) {
          // Improved: went from base → indexed (e.g., 3.36x → 1.0x)
          return `↑ ${base.toFixed(2)}x → ${indexed.toFixed(2)}x (better)`;
        }
        // Degraded: went from base → indexed (e.g., 1.0x → 3.36x)
        return `↓ ${base.toFixed(2)}x → ${indexed.toFixed(2)}x (worse)`;
      })();

      const baselineImpact = (() => {
        if (
          summary.base.withinBaseline === undefined ||
          summary.indexed.withinBaseline === undefined
        ) {
          return 'N/A';
        }
        const base = summary.base.withinBaseline;
        const indexed = summary.indexed.withinBaseline;
        const delta = indexed - base;

        if (Math.abs(delta) < 0.05) return '→ (no change)';

        // For within-baseline, lower is better (chosen plan closer to optimal than baseline)
        if (delta < 0) {
          const improvement = Math.abs(delta / base) * 100;
          return `↑ ${base.toFixed(2)}x → ${indexed.toFixed(2)}x (${improvement.toFixed(0)}% better)`;
        }
        return `↓ ${base.toFixed(2)}x → ${indexed.toFixed(2)}x (worse)`;
      })();

      const row =
        `| ${summary.name} ` +
        `| ${corrImpact} ` +
        `| ${optImpact} ` +
        `| ${baselineImpact} |`;
      console.log(row);
    }
    console.log(
      '\n↑ = Improved with indexing | ↓ = Degraded with indexing | → = No significant change\n',
    );
  }
}
