const charTable = '0123456789abcdefghijklmnopqrstuv';

export function encode(plain: Uint8Array): string {
  let i = 0;
  let shiftIndex = 0;
  let digit = 0;
  let encoded = '';
  const {length} = plain;

  // byte by byte isn't as pretty as quintet by quintet but tests a bit
  // faster. will have to revisit.
  while (i < length) {
    const current = plain[i];

    if (shiftIndex > 3) {
      digit = current & (0xff >> shiftIndex);
      shiftIndex = (shiftIndex + 5) % 8;
      digit =
        (digit << shiftIndex) |
        ((i + 1 < length ? plain[i + 1] : 0) >> (8 - shiftIndex));
      i++;
    } else {
      digit = (current >> (8 - (shiftIndex + 5))) & 0x1f;
      shiftIndex = (shiftIndex + 5) % 8;
      if (shiftIndex === 0) {
        i++;
      }
    }

    encoded += charTable[digit];
  }

  // No padding!
  return encoded;
}
