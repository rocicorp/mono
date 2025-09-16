/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
export class URLParams {
  readonly url: URL;

  constructor(url: URL) {
    this.url = url;
  }

  get(name: string, required: true): string;
  get(name: string, required: boolean): string | null;
  get(name: string, required: boolean) {
    const value = this.url.searchParams.get(name);
    if (value === '' || value === null) {
      if (required) {
        throw new Error(`invalid querystring - missing ${name}`);
      }
      return null;
    }
    return value;
  }

  getInteger(name: string, required: true): number;
  getInteger(name: string, required: boolean): number | null;
  getInteger(name: string, required: boolean) {
    const value = this.get(name, required);
    if (value === null) {
      return null;
    }
    const int = parseInt(value);
    if (isNaN(int)) {
      throw new Error(
        `invalid querystring parameter ${name}, got: ${value}, url: ${this.url}`,
      );
    }
    return int;
  }

  getBoolean(name: string): boolean {
    const value = this.get(name, false);
    if (value === null) {
      return false;
    }
    return value === 'true';
  }
}
