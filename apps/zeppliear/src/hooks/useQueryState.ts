import {useCallback, useEffect, useMemo, useState} from 'react';

export type QueryStateProcessor<T> = {
  toString: (value: T) => string;
  fromString: (value: string | null) => T | null;
};

export const identityProcessor = {
  toString: (value: string) => value,
  fromString: (value: string | null) => value,
};

const queryStateListeners = new Set<() => void>();

export function useQueryState<T>(
  key: string,
  processor: QueryStateProcessor<T>,
) {
  const getQueryValue = useCallback(() => {
    const searchParams = new URLSearchParams(window.location.search);
    const param = searchParams.get(key);
    return param === null ? null : param;
  }, [key]);

  // Initialize state from the current URL
  const [value, setValue] = useState<string | null>(getQueryValue);

  // Update URL when state changes
  useEffect(() => {
    const searchParams = new URLSearchParams(window.location.search);
    const oldRelativePathQuery = `${
      window.location.pathname
    }?${searchParams.toString()}`;
    if (value === null) {
      searchParams.delete(key);
    } else {
      searchParams.set(key, value);
    }
    const newRelativePathQuery = `${
      window.location.pathname
    }?${searchParams.toString()}`;
    if (oldRelativePathQuery === newRelativePathQuery) {
      return;
    }
    history.pushState(null, '', newRelativePathQuery);
    for (const listener of queryStateListeners) {
      listener();
    }
  }, [key, value, processor]);

  useEffect(() => {
    const handlePopState = () => {
      const encoded = getQueryValue();
      setValue(encoded);
    };

    // Subscribe to popstate event
    window.addEventListener('popstate', handlePopState);
    queryStateListeners.add(handlePopState);

    // Cleanup listener
    return () => {
      window.removeEventListener('popstate', handlePopState);
      queryStateListeners.delete(handlePopState);
    };
  }, [getQueryValue, key, processor]);

  // Wrap setValue with a callback that ensures a new function is not created on every render
  const setQueryState = useCallback(
    (newValue: T | null) => {
      const encoded = newValue === null ? null : processor.toString(newValue);
      setValue(encoded);
    },
    [setValue, processor],
  );

  // Memoize the processed query value to avoid rerenders
  const processedQueryValue = useMemo(
    () => processor.fromString(value),
    [value, processor],
  );

  return [processedQueryValue, setQueryState] as const;
}

export default useQueryState;
