export class QueryParseError extends Error {
  readonly cause: unknown;

  constructor(opts: ErrorOptions) {
    super(
      opts?.cause instanceof Error
        ? `Failed to parse arguments for query: ${opts.cause.message}`
        : `Failed to parse arguments for query`,
      opts,
    );
    this.name = 'QueryParseError';
  }
}
