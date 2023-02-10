export class AbortError extends Error {
  name = 'AbortError';
  constructor(message?: string) {
    super(message);
  }
}
