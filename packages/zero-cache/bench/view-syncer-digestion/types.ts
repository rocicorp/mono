export type DigestScenario = {
  readonly name: string;
  readonly transactions: number;
  readonly rowsPerTransaction: number;
  readonly activeRowsPerTransaction: number;
};

export type DigestStats = {
  readonly elapsedMs: number;
  readonly changeLogEntries: number;
  readonly selectedChangeLogEntries: number;
  readonly materializedChanges: number;
  readonly activeChanges: number;
  readonly viewSyncerCount: number;
  readonly p50ViewSyncerMs: number;
  readonly p95ViewSyncerMs: number;
  readonly rowsPerSecond: number;
};

export type ScenarioSummary = {
  readonly name: string;
  readonly transactions: number;
  readonly rowsPerTransaction: number;
  readonly activeRowsPerTransaction: number;
  readonly totalRows: number;
  readonly activeRows: number;
  readonly old: DigestStats;
  readonly filtered: DigestStats;
  readonly speedup: number;
  readonly elapsedDeltaMs: number;
  readonly materializedChangeDelta: number;
};

export type Summary = {
  readonly name: 'zero-cache-view-syncer-digestion';
  readonly generatedAt: string;
  readonly viewSyncerCount: number;
  readonly scenarios: readonly ScenarioSummary[];
};
