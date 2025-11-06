/**
 * Calculate Spearman's rank correlation coefficient between two arrays.
 *
 * Spearman's correlation measures the monotonic relationship between two variables.
 * A value of 1 indicates perfect positive correlation, -1 indicates perfect negative
 * correlation, and 0 indicates no correlation.
 *
 * @param xs - First array of numbers
 * @param ys - Second array of numbers (must be same length as xs)
 * @returns Spearman's rank correlation coefficient in range [-1, 1]
 */
export function spearmanCorrelation(xs: number[], ys: number[]): number {
  if (xs.length !== ys.length) {
    throw new Error('Arrays must have the same length');
  }

  if (xs.length === 0) {
    throw new Error('Arrays must not be empty');
  }

  // Convert values to ranks
  const xRanks = getRanks(xs);
  const yRanks = getRanks(ys);

  // Calculate Pearson correlation of ranks
  return pearsonCorrelation(xRanks, yRanks);
}

/**
 * Convert an array of values to ranks.
 * Ties are assigned the average rank.
 *
 * @param values - Array of numbers to rank
 * @returns Array of ranks (1-based)
 */
function getRanks(values: number[]): number[] {
  // Create array of {value, originalIndex}
  const indexed = values.map((value, i) => ({value, index: i}));

  // Sort by value
  indexed.sort((a, b) => a.value - b.value);

  // Assign ranks (handling ties with average rank)
  const ranks = Array.from<number>({length: values.length});
  let i = 0;
  while (i < indexed.length) {
    // Find all elements with the same value (ties)
    let j = i;
    while (j < indexed.length && indexed[j].value === indexed[i].value) {
      j++;
    }

    // Assign average rank to all tied elements
    const averageRank = (i + j + 1) / 2; // +1 because ranks are 1-based
    for (let k = i; k < j; k++) {
      ranks[indexed[k].index] = averageRank;
    }

    i = j;
  }

  return ranks;
}

/**
 * Calculate Pearson correlation coefficient between two arrays.
 *
 * @param xs - First array of numbers
 * @param ys - Second array of numbers (must be same length as xs)
 * @returns Pearson correlation coefficient in range [-1, 1]
 */
function pearsonCorrelation(xs: number[], ys: number[]): number {
  const n = xs.length;

  // Calculate means
  const xMean = xs.reduce((sum, x) => sum + x, 0) / n;
  const yMean = ys.reduce((sum, y) => sum + y, 0) / n;

  // Calculate covariance and standard deviations
  let covariance = 0;
  let xVariance = 0;
  let yVariance = 0;

  for (let i = 0; i < n; i++) {
    const xDiff = xs[i] - xMean;
    const yDiff = ys[i] - yMean;
    covariance += xDiff * yDiff;
    xVariance += xDiff * xDiff;
    yVariance += yDiff * yDiff;
  }

  // Handle edge case: all values are the same
  if (xVariance === 0 || yVariance === 0) {
    return 0;
  }

  // Calculate correlation coefficient
  return covariance / Math.sqrt(xVariance * yVariance);
}
