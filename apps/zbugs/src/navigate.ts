import {navigate as wouterNavigate} from 'wouter/use-browser-location';

type Options = Parameters<typeof wouterNavigate>[1];
type To = Parameters<typeof wouterNavigate>[0] | URLSearchParams;

/**
 * Navigate to a new location in the application.
 *
 * This extends wouter's navigate to also accept a URLSearchParams object,
 * which will be converted to a query string.
 *
 * @param to - The destination path string or URLSearchParams object
 * @param options - Optional navigation options (e.g., replace)
 */
export function navigate(to: To, options?: Options) {
  if (to instanceof URLSearchParams) {
    wouterNavigate(`?${to}`, options);
  } else {
    wouterNavigate(to, options);
  }
}

/**
 * Add a parameter to a URLSearchParams object.
 *
 * @param qs - The URLSearchParams object to modify
 * @param key - The parameter key to add
 * @param value - The parameter value to add
 * @param mode - If 'exclusive', replaces any existing values for the key; otherwise appends
 * @returns A new URLSearchParams object with the parameter added
 */
export function appendParam(
  qs: URLSearchParams,
  key: string,
  value: string,
): URLSearchParams {
  const newParams = new URLSearchParams(qs);
  newParams.append(key, value);
  return newParams;
}

export function setParam(
  qs: URLSearchParams,
  key: string,
  value: string,
): URLSearchParams {
  const newParams = new URLSearchParams(qs);
  newParams.set(key, value);
  return newParams;
}

/**
 * Remove a parameter from a URLSearchParams object.
 *
 * @param qs - The URLSearchParams object to modify
 * @param key - The parameter key to remove
 * @param value - Optional specific value to remove; if omitted, removes all values for the key
 * @returns A new URLSearchParams object with the parameter removed
 */
export function removeParam(
  qs: URLSearchParams,
  key: string,
  value?: string,
): URLSearchParams {
  const searchParams = new URLSearchParams(qs);
  searchParams.delete(key, value);
  return searchParams;
}

export function replaceHistoryState<T>(data: T) {
  history.replaceState(data, '', document.location.href);
}
