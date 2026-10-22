import Testing

@testable import Zero

/// `JSONObject` keeps keys as a JS object keeps its properties.
struct JSONObjectTests {
  static func keys(_ o: JSONObject) -> [[UInt16]] { o.keys.map { Array($0.utf16) } }

  @Test func keepsInsertionOrder() {
    var o: JSONObject = ["b": 1, "a": 2]
    o["c"] = 3
    o["b"] = 4  // replacing keeps the place
    #expect(o.keys == ["b", "a", "c"])
    #expect(o.map(\.value) == [4, 2, 3])
    #expect(o.description == #"{"b":4,"a":2,"c":3}"#)
    o["a"] = nil
    #expect(o.keys == ["b", "c"])
  }

  /// Array indices first, ascending, as `Object.keys` orders them; `"01"`
  /// and 2^32 - 1 are not indices.
  @Test func integerKeysFirst() {
    var o: JSONObject = ["b": 1, "10": 2, "a": 3, "9": 4, "01": 5, "4294967295": 6, "0": 7]
    #expect(o.keys == ["0", "9", "10", "b", "a", "01", "4294967295"])
    o["4294967294"] = 8
    o["3"] = 9
    #expect(o.keys == ["0", "3", "9", "10", "4294967294", "b", "a", "01", "4294967295"])
  }

  @Test func keysCompareByCodeUnit() {
    var o: JSONObject = ["\u{e9}": 1]
    o["e\u{301}"] = 2
    #expect(Self.keys(o) == [[0xE9], [0x65, 0x301]])
    #expect(o["\u{e9}"] == 1)
    #expect(o["e\u{301}"] == 2)
    #expect(JSONObject(["\u{e9}": 1]) != JSONObject(["e\u{301}": 1]))
  }

  @Test func equalityIgnoresOrder() {
    let a: JSONObject = ["x": 1, "y": [1, 2]]
    let b: JSONObject = ["y": [1, 2], "x": 1]
    #expect(a == b)
    #expect(a.hashValue == b.hashValue)
    #expect(a != ["x": 1, "y": [2, 1]])
    #expect(a != ["x": 1])
  }

  /// Large objects keep an index; it must follow inserts, removals, and
  /// integer keys that go in front.
  @Test func largeObjects() {
    var o = JSONObject()
    for i in 0..<40 { o["k\(i)"] = .number(Double(i)) }
    o["5"] = "five"
    o["k3"] = nil
    o["e\u{301}"] = 1
    o["\u{e9}"] = 2
    #expect(o.count == 42)
    #expect(o.keys.first == "5")
    #expect(o["5"] == "five")
    #expect(o["k3"] == nil)
    for i in 0..<40 where i != 3 { #expect(o["k\(i)"] == .number(Double(i))) }
    #expect(o["e\u{301}"] == 1)
    #expect(o["\u{e9}"] == 2)
    let shuffled = JSONObject(o.reversed().map { (key: $0.key, value: $0.value) })
    #expect(shuffled == o)
  }

  @Test func parsesAndStringifiesInJSOrder() throws {
    let text = #"{"b":1,"a":{"z":null,"y":[]},"2":true,"1":"x","\u00e9":1,"e\u0301":2}"#
    let value = try JSONParser.parse(text)
    #expect(value.description == #"{"1":"x","2":true,"b":1,"a":{"z":null,"y":[]},"é":1,"é":2}"#)
    // The last of duplicate keys wins, in the first one's place.
    #expect(try JSONParser.parse(#"{"a":1,"b":2,"a":3}"#).description == #"{"a":3,"b":2}"#)
  }

  @Test func encodesFieldsInOrder() throws {
    struct S: Codable { var zeta = 1, alpha = "a", mid: [Int] = [] }
    let value = try JSONValueEncoder.encode(S())
    #expect(value.objectValue?.keys == ["zeta", "alpha", "mid"])
  }
}
