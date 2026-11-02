let warnedAboutFallback = false;

type RandomArray = Uint8Array<ArrayBuffer> | Uint32Array<ArrayBuffer>;

type ExpoGlobal = {
  expo?:
    | {
        modules?:
          | {
              ExpoCrypto?:
                | {getRandomValues?: ((array: RandomArray) => void) | undefined}
                | undefined;
            }
          | undefined;
      }
    | undefined;
};

/**
 * Fills `array` with random values. Uses `crypto.getRandomValues` when the
 * runtime provides it, then the native `ExpoCrypto` module when running in an
 * Expo app, and falls back to `Math.random` otherwise, logging a warning the
 * first time the fallback is used.
 *
 * React Native's Hermes engine has no WebCrypto. Expo installs its native
 * modules on `globalThis.expo.modules`, and `ExpoCrypto` is there in Expo Go
 * and in any app that has `expo-crypto` installed, even if the app never
 * imports it. Other React Native apps should import a polyfill such as
 * `react-native-get-random-values` before creating IDs.
 *
 * The globals are looked up on every call rather than at module load: some
 * runtimes (e.g. Cloudflare Workers) only allow generating random values
 * while handling a request.
 */
export function getRandomValues<T extends RandomArray>(array: T): T {
  const {crypto} = globalThis;
  if (typeof crypto?.getRandomValues === 'function') {
    return crypto.getRandomValues(array);
  }

  if (getRandomValuesFromExpo(array)) {
    return array;
  }

  if (!warnedAboutFallback) {
    warnedAboutFallback = true;
    // oxlint-disable-next-line no-console
    console.warn(
      'crypto.getRandomValues is not available; falling back to Math.random for IDs. ' +
        'On React Native, install expo-crypto or import react-native-get-random-values.',
    );
  }
  const max = 2 ** (array.BYTES_PER_ELEMENT * 8);
  for (let i = 0; i < array.length; i++) {
    array[i] = Math.floor(Math.random() * max);
  }
  return array;
}

function getRandomValuesFromExpo(array: RandomArray): boolean {
  try {
    const expoCrypto = (globalThis as ExpoGlobal).expo?.modules?.ExpoCrypto;
    if (typeof expoCrypto?.getRandomValues === 'function') {
      expoCrypto.getRandomValues(array);
      return true;
    }
  } catch {
    // Loading a native module can throw (Expo's own lookup guards against
    // this too). Use the fallback.
  }
  return false;
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
