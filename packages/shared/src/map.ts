const nativeSupport =
  typeof (Map.prototype as unknown as MapES2026<unknown, unknown>)
    .getOrInsert === 'function';

interface MapES2026<K, V> {
  getOrInsert(key: K, defaultValue: V): V;
  getOrInsertComputed(key: K, compute: (key: K) => V): V;
}

/**
 * Returns the value for {@link key} in {@link map}.  If no mapping exists,
 * inserts {@link defaultValue} and returns it.
 *
 * Mirrors the ES2026 `Map.prototype.getOrInsert` proposal.
 */
function getOrInsertPolyfill<K, V>(map: Map<K, V>, key: K, defaultValue: V): V {
  const existing = map.get(key);
  if (existing !== undefined) {
    return existing;
  }
  map.set(key, defaultValue);
  return defaultValue;
}

/**
 * Returns the value for {@link key} in {@link map}.  If no mapping exists,
 * calls {@link compute} with the key, inserts the result, and returns it.
 *
 * Mirrors the ES2026 `Map.prototype.getOrInsertComputed` proposal.
 */
function getOrInsertComputedPolyfill<K, V>(
  map: Map<K, V>,
  key: K,
  compute: (key: K) => V,
): V {
  const existing = map.get(key);
  if (existing !== undefined) {
    return existing;
  }
  const value = compute(key);
  map.set(key, value);
  return value;
}

function getOrInsertNative<K, V>(map: Map<K, V>, key: K, defaultValue: V): V {
  return (map as unknown as MapES2026<K, V>).getOrInsert(key, defaultValue);
}

function getOrInsertComputedNative<K, V>(
  map: Map<K, V>,
  key: K,
  compute: (key: K) => V,
): V {
  return (map as unknown as MapES2026<K, V>).getOrInsertComputed(key, compute);
}

export const getOrInsert = nativeSupport
  ? getOrInsertNative
  : getOrInsertPolyfill;

export const getOrInsertComputed = nativeSupport
  ? getOrInsertComputedNative
  : getOrInsertComputedPolyfill;
