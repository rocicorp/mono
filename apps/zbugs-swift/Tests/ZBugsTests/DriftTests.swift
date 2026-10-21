import Foundation
import Testing
import ZBugs
import Zero
import ZeroTesting

/// zbugs' Swift queries and mutators do what the TS ones do on the samples
/// in drift-samples.ts.
@Suite @MainActor struct DriftTests {
  @Test func matchesTheTSQueriesAndMutators() throws {
    let vectors = try String(
      contentsOf: URL(fileURLWithPath: #filePath).deletingLastPathComponent()
        .appendingPathComponent("drift-vectors.json"), encoding: .utf8)
    let failures = try Drift.failures(
      vectors: vectors, schema: .zbugs, queries: Queries.all, mutators: Mutators.all)
    #expect(failures == [])
  }
}
