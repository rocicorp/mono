import Zero

/// A one-table schema for the Apple layer's tests.
struct Note: ZeroRow, Hashable, Identifiable {
  var id: String
  var text: String

  init(id: String, text: String) {
    self.id = id
    self.text = text
  }

  static let tableName = "note"
  static let columns: KeyPathNames<Note> = [\Note.id: "id", \Note.text: "text"]
  static let relationships: KeyPathNames<Note> = [:]

  init(entry: ViewEntry) throws(ZeroValueError) {
    id = try entry.column("id")
    text = try entry.column("text")
  }

  var columnValues: Row { ["id": id.zeroValue, "text": text.zeroValue] }
}

extension ZeroSchema {
  static let notes = ZeroSchema(tables: [
    TableSchema(
      name: "note", columns: [("id", ColumnSchema(.string)), ("text", ColumnSchema(.string))],
      primaryKey: ["id"])
  ])
}

enum Notes {
  static let table = Query<Note, [Note]>(AnyQuery(.notes, "note"))
  static let all = defineQuery("notes.all") { () in table.orderBy(\.id, .asc) }

  struct Put: Codable, Sendable {
    var id: String
    var text: String
  }

  static let put = defineMutator("note.put") { (tx, note: Put) in
    try tx.mutate.upsert(Note(id: note.id, text: note.text))
  }
  static let remove = defineMutator("note.remove") { (tx, id: String) in
    try tx.mutate.note.delete(["id": .string(id)])
  }
}
