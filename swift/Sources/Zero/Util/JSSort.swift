/// `Array.prototype.sort` as V8 implements it.
///
/// Some comparators the client ports are not consistent (the AST condition
/// comparator orders subqueries before conjunctions from both sides), so the
/// result depends on the exact sequence of comparisons. V8's TimSort, for
/// arrays shorter than its minimum run of 64, finds the leading run (reversing
/// it if strictly descending) and binary-insertion-sorts the rest; that is
/// reproduced here exactly. Longer arrays use the same insertion sort, which
/// matches V8 for consistent comparators (it is stable).
package enum JSSort {
  package static func sorted<T>(_ array: [T], by cmp: (T, T) -> Int) -> [T] {
    var a = array
    sort(&a, by: cmp)
    return a
  }

  package static func sort<T>(_ a: inout [T], by cmp: (T, T) -> Int) {
    let n = a.count
    if n < 2 { return }
    // CountAndMakeRun
    var runLength = 2
    let descending = cmp(a[1], a[0]) < 0
    var previous = a[1]
    var i = 2
    while i < n {
      let current = a[i]
      let order = cmp(current, previous)
      if descending {
        if order >= 0 { break }
      } else {
        if order < 0 { break }
      }
      previous = current
      runLength += 1
      i += 1
    }
    if descending {
      a[0..<runLength].reverse()
    }
    // BinaryInsertionSort(low = 0, startArg = runLength, high = n)
    var start = runLength
    while start < n {
      let pivot = a[start]
      var left = 0
      var right = start
      while left < right {
        let mid = left + ((right - left) >> 1)
        let order = cmp(pivot, a[mid])
        if order < 0 {
          right = mid
        } else {
          left = mid + 1
        }
      }
      var p = start
      while p > left {
        a[p] = a[p - 1]
        p -= 1
      }
      a[left] = pivot
      start += 1
    }
  }
}
