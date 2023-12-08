export function encodeHeaderValue(value: string): string {
  // encodeURIComponent escapes the following chars which are allowed
  // in header values.
  // : ; , / " ? { } [ ] @ < > = + # $ & ` | ^ space and %
  // Unescape all of them expect %, to make the encoded value smaller and more
  // readable.  Do not unescape % as that would break decoding of the
  // percent decoding done by encodeURIComponent.
  return encodeURIComponent(value).replace(
    /%(3A|3B|2C|2F|22|3F|7B|7D|5B|5D|40|3C|3E|3D|2B|23|24|26|60|7C|5E|20)/g,
    (_, hex) => String.fromCharCode(parseInt(hex, 16)),
  );
}

export function decodeHeaderValue(encoded: string): string {
  return decodeURIComponent(encoded);
}

export function getBearerToken(
  headers: Headers,
):
  | [decodedAuth: string, errorResponse: undefined]
  | [decodedAuth: undefined, errorResponse: Response] {
  const err = (msg: string, status: number): [undefined, Response] => [
    undefined,
    new Response(msg, {status}),
  ];

  const authHeader = headers.get('Authorization');
  if (!authHeader) {
    return [
      undefined,
      new Response('Missing Authorization header', {status: 401}),
    ];
  }

  if (!authHeader.startsWith('Bearer ')) {
    return err('Invalid Authorization header', 401);
  }
  const encodedAuth = authHeader.slice('Bearer '.length);
  try {
    return [decodeHeaderValue(encodedAuth), undefined];
  } catch {
    return err('Malformed Authorization header', 401);
  }
}
