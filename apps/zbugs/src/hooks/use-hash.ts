import {useLocationProperty} from 'wouter/use-browser-location';

export function useHash() {
  const hash = useLocationProperty(() => location.hash, undefined);
  return hash.slice(1);
}
