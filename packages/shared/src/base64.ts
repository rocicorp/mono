// Unpadded base64url (RFC 4648 §5: base64 with `-` and `_` instead of `+` and
// `/`) for Uint8Arrays. Uses the standard `Uint8Array.prototype.toBase64` and
// `Uint8Array.fromBase64` when the runtime has them, and btoa/atob otherwise
// (Node 22, older browsers). Node's Buffer isn't used: React Native apps
// often polyfill it with the `buffer` package, which has no base64url.

type Base64urlOptions = {alphabet: 'base64url'; omitPadding?: boolean};

const PADDING = /=+$/;
// Whole groups of four characters, then an unpadded or correctly padded last
// group.
const BASE64URL = /^(?:[\w-]{4})*(?:[\w-]{2}(?:==)?|[\w-]{3}=?)?$/;

/** Encodes `bytes` as unpadded base64url. */
export function toBase64url(bytes: Uint8Array): string {
  const native = bytes as Uint8Array & {
    toBase64?: (options: Base64urlOptions) => string;
  };
  return native.toBase64
    ? native.toBase64({alphabet: 'base64url', omitPadding: true})
    : toBase64urlWithBtoa(bytes);
}

/**
 * Decodes base64url, padded or not.
 *
 * @throws SyntaxError if `encoded` is not base64url, including when it has
 *   whitespace, which the standard and atob skip. atob doesn't reject
 *   everything the standard rejects, so this checks first.
 */
export function fromBase64url(encoded: string): Uint8Array {
  if (!BASE64URL.test(encoded)) {
    throw new SyntaxError('Invalid base64url');
  }
  const native = Uint8Array as {
    fromBase64?: (s: string, options: Base64urlOptions) => Uint8Array;
  };
  return native.fromBase64
    ? native.fromBase64(encoded, {alphabet: 'base64url'})
    : fromBase64urlWithAtob(encoded);
}

/** @visibleForTesting */
export function toBase64urlWithBtoa(bytes: Uint8Array): string {
  // A local, so the loop doesn't look it up on String for every byte
  // (Hermes).
  const {fromCharCode} = String;
  // One character per byte. Spreading a large array into fromCharCode could
  // overflow the stack.
  return btoa(Array.from(bytes, b => fromCharCode(b)).join(''))
    .replaceAll('+', '-')
    .replaceAll('/', '_')
    .replace(PADDING, '');
}

/**
 * Decodes base64url that {@link fromBase64url} has checked.
 *
 * @visibleForTesting
 */
export function fromBase64urlWithAtob(s: string): Uint8Array {
  return Uint8Array.from(atob(s.replaceAll('-', '+').replaceAll('_', '/')), c =>
    c.charCodeAt(0),
  );
}
