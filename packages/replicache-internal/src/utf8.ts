const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export function encode(text: string): Uint8Array {
  return textEncoder.encode(text);
}

export function decode(bytes: Uint8Array): string {
  return textDecoder.decode(bytes);
}
