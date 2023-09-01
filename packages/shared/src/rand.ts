/**
 * @param min Inclusive minimum of the result.
 * @param max Inclusive maximum of the result.
 * @returns A random integer in the (inclusive) range of [`min`, `max`].
 */
export function randInt(min: number, max: number) {
  min = Math.ceil(min);
  max = Math.floor(max);
  return Math.floor(Math.random() * (max - min + 1) + min);
}
