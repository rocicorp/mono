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
