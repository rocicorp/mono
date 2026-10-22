import Testing

@testable import Zero

func double(fromBits hex: String) -> Double {
  Double(bitPattern: UInt64(hex, radix: 16)!)
}

@Suite(.enabled(if: Vectors.available))
struct JSONConformanceTests {
  struct NumberFormat: Decodable {
    let bits: String
    let string: String
  }

  @Test func numberFormat() throws {
    for c in try Vectors.load("number-format", as: NumberFormat.self) {
      #expect(JSNumber.format(double(fromBits: c.bits)) == c.string, "bits \(c.bits)")
    }
  }

  struct NumberParse: Decodable {
    let input: String
    let bits: String
  }

  @Test func numberParse() throws {
    for c in try Vectors.load("number-parse", as: NumberParse.self) {
      let got = JSNumber.parse(c.input)
      let want = double(fromBits: c.bits)
      #expect(
        got.bitPattern == want.bitPattern || (got.isNaN && want.isNaN),
        "Number(\(String(reflecting: c.input))) = \(got), want \(want)")
    }
  }

  struct Parse: Decodable {
    let input: String
    let ok: Bool
    let stringify: String?
  }

  @Test func parse() throws {
    for c in try Vectors.load("json-parse", as: Parse.self) {
      let result = Result { () throws(JSONParser.Error) in try JSONParser.parse(c.input) }
      switch result {
      case .success(let value):
        #expect(c.ok, "accepted \(String(reflecting: c.input))")
        if let stringify = c.stringify {
          // The same text, keys in JS property order, by code unit.
          let ours = JSONSerializer.stringify(value)
          #expect(
            Array(ours.utf16) == Array(stringify.utf16),
            "\(String(reflecting: c.input)): Swift \(ours.debugDescription), JS \(stringify.debugDescription)"
          )
        }
      case .failure:
        #expect(!c.ok, "rejected \(String(reflecting: c.input))")
      }
    }
  }

  struct Stringify: Decodable {
    let input: String
    let stringify: String
  }

  @Test func stringifyStrings() throws {
    for c in try Vectors.load("json-stringify-strings", as: Stringify.self) {
      #expect(
        JSONSerializer.stringify(.string(c.input)) == c.stringify, "\(String(reflecting: c.input))")
    }
  }
}
