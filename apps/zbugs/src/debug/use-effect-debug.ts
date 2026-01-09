import React, {useEffect, useRef} from 'react';

export function useEffectDebug(
  effect: React.EffectCallback,
  deps: Record<string, unknown>,
  label?: string,
) {
  const prevDepsRef = useRef<Record<string, unknown>>();
  const depsArray = Object.values(deps);

  useEffect(() => {
    if (prevDepsRef.current) {
      const changes: Record<string, unknown> = {};
      let changed = false;
      for (const [key, value] of Object.entries(deps)) {
        if (prevDepsRef.current[key] !== value) {
          changes[key] = value;
          changed = true;
        }
      }
      if (changed) {
        // oxlint-disable-next-line no-console
        console.debug(label || 'useEffect changes:', changes);
      }
    }
    prevDepsRef.current = deps;
    return effect();
  }, depsArray);
}
