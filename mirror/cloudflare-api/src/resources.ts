import type {PartialDeep} from 'type-fest';
import {cfFetch} from './fetch.js';
import {assert} from 'shared/src/asserts.js';

export type ListFn<T> = (query?: URLSearchParams) => Promise<T[]>;
export type GetFn<T> = (id: string) => Promise<T>;
export type PostFn<I, O = I> = (val: PartialDeep<I>) => Promise<O>;
export type PutFn<I, O = I> = (id: string, val: PartialDeep<I>) => Promise<O>;
export type PatchFn<I, O = I> = PutFn<I, O>;
export type DeleteFn<T = unknown> = (id: string) => Promise<T>;

const headers = {'Content-Type': 'application/json'};

export class Resource {
  readonly #apiToken: string;
  readonly #url: string;

  constructor(apiToken: string, url: string) {
    this.#apiToken = apiToken;
    this.#url = url;
  }

  append(path: string): Resource {
    assert(!path.startsWith('/'));
    return new Resource(this.#apiToken, `${this.#url}/${path}`);
  }

  #fetch<ResultType>(
    method: 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE',
    body?: BodyInit,
    query?: URLSearchParams,
  ) {
    if (body) {
      console.debug(`${method} ${this.#url}: ${body}`);
    }
    return cfFetch<ResultType>(
      this.#apiToken,
      this.#url,
      {
        method,
        headers,
        body: body ?? null,
      },
      query,
    );
  }

  readonly get = <T>(query?: URLSearchParams): Promise<T> =>
    this.#fetch<T>('GET', undefined, query);

  readonly post = <I, O = I>(body: PartialDeep<I>): Promise<O> =>
    this.#fetch('POST', JSON.stringify(body));

  readonly put = <I, O = I>(body: PartialDeep<I>): Promise<O> =>
    this.#fetch('PUT', JSON.stringify(body));

  readonly patch = <I, O = I>(body: PartialDeep<I>): Promise<O> =>
    this.#fetch('PATCH', JSON.stringify(body));

  readonly delete = <T>(): Promise<T> => this.#fetch('DELETE');
}
