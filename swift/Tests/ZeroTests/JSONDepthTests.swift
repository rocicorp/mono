import Foundation
import Testing

@testable import Zero

/// JSON nested as deep as the parser accepts doesn't overflow the stack of a
/// thread with 512 KB, the size of a test or cooperative thread on macOS
/// (the iPhone main thread has 1 MB).
struct JSONDepthTests {
  @Test func deepestValuesOnSmallStack() async throws {
    let depth = JSONParser.maxDepth
    let array = String(repeating: "[", count: depth) + String(repeating: "]", count: depth)
    let object =
      String(repeating: #"{"a":"#, count: depth - 1) + "{}"
      + String(repeating: "}", count: depth - 1)
    // The same, but for the innermost value.
    let otherArray =
      String(repeating: "[", count: depth - 1) + "[0]" + String(repeating: "]", count: depth - 1)
    let otherObject =
      String(repeating: #"{"a":"#, count: depth - 1) + #"{"a":0}"#
      + String(repeating: "}", count: depth - 1)
    let deep = try await onThread(stackSize: 512 << 10) {
      var values: [JSONValue] = []
      var results: [Bool] = []
      for (text, otherText) in [(array, otherArray), (object, otherObject)] {
        let value = try JSONParser.parse(text)
        let copy = try JSONParser.parse(text)
        let other = try JSONParser.parse(otherText)
        results.append(JSONSerializer.stringify(value) == text)
        results.append(value == copy)
        results.append(value != other)
        results.append(value.hashValue == copy.hashValue)
        results.append(
          QueryHash.hashOfNameAndArgs("q", [value]) == QueryHash.hashOfNameAndArgs("q", [copy]))
        values += [value, copy, other]
      }
      // One more level is rejected.
      var errors: [JSONParser.Error?] = []
      for text in ["[" + array + "]", #"{"a":"# + object + "}"] {
        do throws(JSONParser.Error) {
          _ = try JSONParser.parse(text)
          errors.append(nil)
        } catch {
          errors.append(error)
        }
      }
      return Deep(values: values, results: results, errors: errors)
    }
    #expect(deep.results == Array(repeating: true, count: 10))
    #expect(
      deep.errors == [
        JSONParser.Error(message: "JSON nested too deeply", offset: depth),
        JSONParser.Error(message: "JSON nested too deeply", offset: depth * 5),
      ])
    // Freeing still recurses, in `Array`'s and `JSONObject`'s deinit. That
    // fits in 512 KB in a release build, but not for the object in a debug
    // build (about 1,000 levels fit), so the values are freed on a larger
    // stack.
    try await onThread(stackSize: 8 << 20) { deep.values = [] }
  }

  final class Deep: @unchecked Sendable {
    var values: [JSONValue]
    let results: [Bool]
    let errors: [JSONParser.Error?]

    init(values: [JSONValue], results: [Bool], errors: [JSONParser.Error?]) {
      self.values = values
      self.results = results
      self.errors = errors
    }
  }

  /// Runs `body` on a thread with a stack of `stackSize` bytes.
  func onThread<T: Sendable>(
    stackSize: Int, _ body: @escaping @Sendable () throws -> T
  ) async throws -> T {
    try await withCheckedThrowingContinuation { (done: CheckedContinuation<T, any Error>) in
      let thread = Thread { done.resume(with: Result { try body() }) }
      thread.stackSize = stackSize
      thread.start()
    }
  }
}
