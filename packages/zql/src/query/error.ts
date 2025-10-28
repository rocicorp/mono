export class QueryParseError extends Error {
  readonly cause: unknown;

  constructor(queryName: string, opts: ErrorOptions) {
    super(
      opts?.cause instanceof Error
        ? `Failed to parse arguments for query "${queryName}": ${opts.cause.message}`
        : `Failed to parse arguments for query "${queryName}"`,
      opts,
    );
    this.name = 'QueryParseError';
  }
}
