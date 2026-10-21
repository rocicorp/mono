import Foundation
import Zero

/// Locates and decodes the vector files in `packages/zero-conformance/vectors`.
///
/// The vectors live in the monorepo, outside this package. The mirror
/// repository doesn't carry them, so every conformance suite is skipped when
/// they aren't found.
enum Vectors {
  static let directory: URL? = {
    if let override = ProcessInfo.processInfo.environment["ZERO_CONFORMANCE_VECTORS"] {
      return URL(fileURLWithPath: override, isDirectory: true)
    }
    let url = URL(fileURLWithPath: #filePath)
      .deletingLastPathComponent()  // ConformanceTests
      .deletingLastPathComponent()  // Tests
      .deletingLastPathComponent()  // swift
      .deletingLastPathComponent()  // repo root
      .appendingPathComponent("packages/zero-conformance/vectors", isDirectory: true)
    return FileManager.default.fileExists(atPath: url.path) ? url : nil
  }()

  static var available: Bool { directory != nil }

  /// `packages/zero-conformance/generated`: seeded fuzz corpora that are not
  /// checked in. Run `pnpm run generate` there (CI does) to create them.
  static let generatedDirectory: URL? = {
    guard let directory else { return nil }
    let url = directory.deletingLastPathComponent().appendingPathComponent(
      "generated", isDirectory: true)
    return FileManager.default.fileExists(atPath: url.path) ? url : nil
  }()

  static var generatedAvailable: Bool { generatedDirectory != nil }

  static func generatedCases(_ name: String) throws -> [JSONValue] {
    guard let generatedDirectory else { throw VectorError.missingDirectory }
    let data = try Data(contentsOf: generatedDirectory.appendingPathComponent("\(name).json"))
    let file = try JSONParser.parse(Array(data))
    return file["cases"]?.arrayValue ?? []
  }

  /// A whole vector file, including its `shared` fixtures.
  static func file(_ name: String) throws -> JSONValue {
    let file = try JSONParser.parse(Array(try data(name)))
    guard file["formatVersion"] == .number(Double(supportedFormatVersion)) else {
      throw VectorError.unsupportedFormat(
        name: name, version: Int(file["formatVersion"]?.numberValue ?? -1))
    }
    return file
  }

  static func generatedFile(_ name: String) throws -> JSONValue {
    guard let generatedDirectory else { throw VectorError.missingDirectory }
    let data = try Data(contentsOf: generatedDirectory.appendingPathComponent("\(name).json"))
    return try JSONParser.parse(Array(data))
  }

  struct File<Case: Decodable>: Decodable {
    let formatVersion: Int
    let kind: String
    let description: String
    let cases: [Case]
  }

  static let supportedFormatVersion = 1

  static func load<Case: Decodable>(_ name: String, as _: Case.Type = Case.self) throws -> [Case] {
    guard let directory else { throw VectorError.missingDirectory }
    let data = try Data(contentsOf: directory.appendingPathComponent("\(name).json"))
    let file = try JSONDecoder().decode(File<Case>.self, from: data)
    guard file.formatVersion == supportedFormatVersion else {
      throw VectorError.unsupportedFormat(name: name, version: file.formatVersion)
    }
    return file.cases
  }

  /// The cases of a vector file parsed with the client's own JSON parser,
  /// for vectors that carry arbitrary JSON values.
  static func cases(_ name: String) throws -> [JSONValue] {
    let file = try JSONParser.parse(Array(try data(name)))
    guard file["formatVersion"] == .number(Double(supportedFormatVersion)) else {
      throw VectorError.unsupportedFormat(
        name: name, version: Int(file["formatVersion"]?.numberValue ?? -1))
    }
    return file["cases"]?.arrayValue ?? []
  }

  /// Raw bytes of a vector file, for suites that decode it with the client's
  /// own JSON parser.
  static func data(_ name: String) throws -> Data {
    guard let directory else { throw VectorError.missingDirectory }
    return try Data(contentsOf: directory.appendingPathComponent("\(name).json"))
  }
}

enum VectorError: Error, CustomStringConvertible {
  case missingDirectory
  case unsupportedFormat(name: String, version: Int)

  var description: String {
    switch self {
    case .missingDirectory:
      "packages/zero-conformance/vectors not found; set ZERO_CONFORMANCE_VECTORS"
    case .unsupportedFormat(let name, let version):
      "\(name).json has format version \(version); this runner supports \(Vectors.supportedFormatVersion)"
    }
  }
}
