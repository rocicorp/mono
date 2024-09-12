export function randomUint64(): bigint {
  // Generate two random 32-bit unsigned integers using Math.random()
  const high = Math.floor(Math.random() * 0xffffffff); // High 32 bits
  const low = Math.floor(Math.random() * 0xffffffff); // Low 32 bits

  // Combine the high and low parts to form a 64-bit unsigned integer
  return (BigInt(high) << 32n) | BigInt(low);
}
