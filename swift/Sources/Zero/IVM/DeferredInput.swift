/// A placeholder input for a view whose pipeline is not built yet
/// (`packages/zql/src/ivm/deferred-input.ts`).
///
/// Until `attach` it behaves like an empty pipeline: `fetch` yields nothing
/// and nothing is pushed. `attach` builds the real pipeline and hydrates the
/// output with its rows as add changes, as if they had just arrived.
package final class DeferredInput: Input {
  private let schema: SourceSchema
  private let build: () throws -> any Input
  private weak var output: (any Output)?
  private var input: (any Input)?
  package private(set) var destroyed = false

  /// `schema` is what the built pipeline reports; `build` builds it, once.
  package init(schema: SourceSchema, build: @escaping () throws -> any Input) {
    self.schema = schema
    self.build = build
  }

  package var attached: Bool { input != nil }

  package func setOutput(_ output: any Output) {
    self.output = output
    input?.setOutput(output)
  }

  package func getSchema() -> SourceSchema { schema }

  package func fetch(_ req: FetchRequest) -> NodeStream {
    input?.fetch(req) ?? .empty
  }

  package func destroy() {
    destroyed = true
    input?.destroy()
  }

  /// Builds the pipeline and pushes its rows to the output. Leaves this
  /// input unattached when building or hydrating throws.
  package func attach() throws {
    precondition(input == nil, "DeferredInput: pipeline already attached")
    precondition(!destroyed, "DeferredInput: cannot attach after destroy")
    let input = try build()
    guard let output else {
      self.input = input
      return
    }
    input.setOutput(output)
    for node in input.fetch(FetchRequest()) {
      output.push(.add(node), pusher: input)
    }
    output.reconcile(pusher: input)
    self.input = input
  }
}
