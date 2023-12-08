export function getRequiredSearchParams(
  keys: string[],
  searchParams: URLSearchParams,
  makeErrorResponse: (message: string) => Response,
):
  | {
      values: string[];
      errorResponse: undefined;
    }
  | {
      values: never[];
      errorResponse: Response;
    } {
  const err = (s: string) => ({
    values: [],
    errorResponse: makeErrorResponse(s),
  });

  const values: string[] = [];
  for (const key of keys) {
    const value = searchParams.get(key);
    if (!value) {
      return err(`${key} parameter required`);
    }
    values.push(value);
  }
  return {values, errorResponse: undefined};
}
