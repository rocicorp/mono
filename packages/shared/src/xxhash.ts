import xxhash from 'xxhash-wasm'

let xxhash64: ((str: string) => void) | null = null

void xxhash().then((x) => {
  xxhash64 = x.h64
})

export function h64(str: string) {
  if (!xxhash64) throw new Error(`Not loaded hash`)
  return xxhash64(str)
}
