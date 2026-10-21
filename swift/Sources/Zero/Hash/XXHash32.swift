/// xxHash32 over UTF-8 bytes, and the wider `h64`/`h128` built from it
/// (`packages/shared/src/hash.ts`).
package enum XXHash32 {
  package static let prime1: UInt32 = 2_654_435_761
  package static let prime2: UInt32 = 2_246_822_519
  package static let prime3: UInt32 = 3_266_489_917
  package static let prime4: UInt32 = 668_265_263
  package static let prime5: UInt32 = 374_761_393

  @inlinable
  package static func round(_ acc: UInt32, _ word: UInt32) -> UInt32 {
    var acc = acc &+ word &* prime2
    acc = (acc << 13) | (acc >> 19)
    return acc &* prime1
  }

  @inlinable
  package static func avalanche(_ acc: UInt32) -> UInt32 {
    var acc = acc
    acc ^= acc >> 15
    acc = acc &* prime2
    acc ^= acc >> 13
    acc = acc &* prime3
    acc ^= acc >> 16
    return acc
  }

  /// Reference xxHash32 of `bytes` with `seed`.
  package static func hash<C: Collection>(_ bytes: C, seed: UInt32 = 0) -> UInt32
  where C.Element == UInt8, C.Index == Int {
    let len = bytes.count
    var offset = bytes.startIndex
    let endIndex = bytes.endIndex
    var acc: UInt32

    @inline(__always) func word(_ o: Int) -> UInt32 {
      UInt32(bytes[o]) | UInt32(bytes[o + 1]) << 8 | UInt32(bytes[o + 2]) << 16
        | UInt32(bytes[o + 3]) << 24
    }

    if len >= 16 {
      var v1 = seed &+ prime1 &+ prime2
      var v2 = seed &+ prime2
      var v3 = seed
      var v4 = seed &- prime1
      let limit = endIndex - 16
      while offset <= limit {
        v1 = round(v1, word(offset))
        v2 = round(v2, word(offset + 4))
        v3 = round(v3, word(offset + 8))
        v4 = round(v4, word(offset + 12))
        offset += 16
      }
      acc =
        ((v1 << 1) | (v1 >> 31)) &+ ((v2 << 7) | (v2 >> 25)) &+ ((v3 << 12) | (v3 >> 20))
        &+ ((v4 << 18) | (v4 >> 14))
    } else {
      acc = seed &+ prime5
    }

    acc = acc &+ UInt32(truncatingIfNeeded: len)

    while offset <= endIndex - 4 {
      acc = acc &+ word(offset) &* prime3
      acc = ((acc << 17) | (acc >> 15)) &* prime4
      offset += 4
    }
    while offset < endIndex {
      acc = acc &+ UInt32(bytes[offset]) &* prime5
      acc = ((acc << 11) | (acc >> 21)) &* prime1
      offset += 1
    }
    return avalanche(acc)
  }

  /// `h32(s)`: xxHash32 of the UTF-8 bytes of `s`, seed 0.
  package static func h32(_ s: String) -> UInt32 {
    var s = s
    return s.withUTF8 { hash($0, seed: 0) }
  }

  /// `h64(s)`: seed-0 digest in the high word, seed-1 in the low word.
  package static func h64(_ s: String) -> UInt64 {
    var s = s
    return s.withUTF8 { buf in
      UInt64(hash(buf, seed: 0)) << 32 | UInt64(hash(buf, seed: 1))
    }
  }

  /// `h128(s)`: digests for seeds 0...3, highest-order word first.
  package static func h128(_ s: String) -> UInt128 {
    var s = s
    return s.withUTF8 { buf in
      var r: UInt128 = 0
      for seed: UInt32 in 0..<4 {
        r = r << 32 | UInt128(hash(buf, seed: seed))
      }
      return r
    }
  }
}
