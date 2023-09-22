import type {PartialDeep} from 'type-fest';
import {cfFetch} from './fetch.js';
import {assert} from 'shared/src/asserts.js';

export type ListFn<T> = (query?: URLSearchParams) => Promise<T[]>;
export type GetOnlyFn<T> = (query?: URLSearchParams) => Promise<T>;
export type GetFn<T> = (id: string, query?: URLSearchParams) => Promise<T>;
export type SetOnlyFn<I, O = I> = (
  val: PartialDeep<I>,
  query?: URLSearchParams,
) => Promise<O>;
export type RawSetOnlyFn<I extends BodyInit, O = undefined> = (
  val: I,
  query?: URLSearchParams,
) => Promise<O>;
export type SetFn<I, O = I> = (
  id: string,
  val: PartialDeep<I>,
  query?: URLSearchParams,
) => Promise<O>;
export type DeleteFn<T = {id: string}> = (
  id: string,
  query?: URLSearchParams,
) => Promise<T>;
export type DeleteOnlyFn<T = {id: string}> = (
  query?: URLSearchParams,
) => Promise<T>;

const JSON_HEADERS = {'Content-Type': 'application/json'} as const;

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
    json = true,
  ) {
    // Simple but sufficient hack to avoid logging sensitive data.
    // There's only one endpoint (so far) where sensitive data is sent.
    if (body && !this.#url.endsWith('/secrets')) {
      console.debug(`${method} ${this.#url}: ${body}`);
    } else {
      console.debug(`${method} ${this.#url}`);
    }
    return cfFetch<ResultType>(
      this.#apiToken,
      this.#url,
      {
        method,
        headers: json ? JSON_HEADERS : {},
        body: body ?? null,
      },
      query,
    );
  }

  readonly get = <T>(query?: URLSearchParams): Promise<T> =>
    this.#fetch<T>('GET', undefined, query);

  readonly post = <I, O = I>(
    body: PartialDeep<I>,
    query?: URLSearchParams,
  ): Promise<O> => this.#fetch('POST', JSON.stringify(body), query);

  readonly rawPost = <I extends BodyInit, O = undefined>(
    body: I,
    query?: URLSearchParams,
  ): Promise<O> => this.#fetch('POST', body, query, false);

  readonly put = <I, O = I>(
    body: PartialDeep<I>,
    query?: URLSearchParams,
  ): Promise<O> => this.#fetch('PUT', JSON.stringify(body), query);

  readonly rawPut = <I extends BodyInit, O = undefined>(
    body: I,
    query?: URLSearchParams,
  ): Promise<O> => this.#fetch('PUT', body, query, false);

  readonly patch = <I, O = I>(
    body: PartialDeep<I>,
    query?: URLSearchParams,
  ): Promise<O> => this.#fetch('PATCH', JSON.stringify(body), query);

  readonly delete = <T>(query?: URLSearchParams): Promise<T> =>
    this.#fetch('DELETE', undefined, query);
}
