let warnedAboutFallback = false;

/**
 * Fills `array` with random values. Uses `crypto.getRandomValues` when the
 * runtime provides it and falls back to `Math.random` otherwise, logging a
 * warning the first time the fallback is used.
 *
 * React Native's Hermes engine has no WebCrypto, so apps running there should
 * install a polyfill such as `react-native-get-random-values` or `expo-crypto`
 * before creating IDs.
 *
 * The `crypto` global is looked up on every call rather than at module load:
 * some runtimes (e.g. Cloudflare Workers) only allow generating random values
 * while handling a request.
 */
export function getRandomValues<
  T extends Uint8Array<ArrayBuffer> | Uint32Array<ArrayBuffer>,
>(array: T): T {
  const {crypto} = globalThis;
  if (typeof crypto?.getRandomValues === 'function') {
    return crypto.getRandomValues(array);
  }

  if (!warnedAboutFallback) {
    warnedAboutFallback = true;
    // oxlint-disable-next-line no-console
    console.warn(
      'crypto.getRandomValues is not available; falling back to Math.random for IDs. ' +
        'On React Native, install react-native-get-random-values or expo-crypto.',
    );
  }
  const max = 2 ** (array.BYTES_PER_ELEMENT * 8);
  for (let i = 0; i < array.length; i++) {
    array[i] = Math.floor(Math.random() * max);
  }
  return array;
}

export function randomCharacters(length: number) {
  let result = '';
  const characters =
    'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  const charactersLength = characters.length;
  let counter = 0;
  while (counter < length) {
    result += characters.charAt(Math.floor(Math.random() * charactersLength));
    counter += 1;
  }
  return result;
}
