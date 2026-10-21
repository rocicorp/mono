import SwiftUI
import ZeroTesting
import ZeroUI
import ZeroVirtual

/// zero-virtual's chat demo (`demo/chat`): it opens at the latest message
/// and follows new ones while it's at the bottom.
struct ChatScreen: View {
  let backend: Backend
  @State private var list: ChatList
  @ZeroQuery(ChatQueries.latest()) private var latest
  @State private var started = false

  init(backend: Backend) {
    self.backend = backend
    _list = State(initialValue: backend.chatList())
  }

  var body: some View {
    VStack(spacing: 0) {
      // Off the safe area's edges, the scroll view's frame is its viewport,
      // which the UI tests measure rows against.
      Divider()
      // On screen once it knows where to open, the latest message: shown
      // before, it would draw the top of the chat for a frame.
      if started, Native.enabled {
        NativeVirtualList(list: list) { item, row in
          let m = item.row
          row.show(id: m.id, first: m.authorId, second: m.text, captionFirst: true, padding: 6)
        }
      } else if started {
        VirtualList(list) { item in
          MessageRow(message: item.row)
        }
        .accessibilityIdentifier("list")
      } else {
        Spacer()
      }
      Divider()
      HStack {
        Button("Receive") { backend.receive() }.accessibilityIdentifier("receive")
        Button("Grow last") { backend.growLast() }.accessibilityIdentifier("grow")
        Button("Latest") { toLatest() }
          .accessibilityIdentifier("latest")
      }
      .padding(8)
    }
    .overlay(alignment: .topTrailing) { ProbeSummary() }
    .onChange(of: latest?.id, initial: true) {
      guard !started, let id = latest?.id else { return }
      started = true
      list.scrollToItem(id, align: .end)
    }
    .task {
      guard FrameProbe.script == "chat" else { return }
      await ProbeScript.chat(
        latestID: { latest?.id }, toLatest: toLatest, receive: { backend.receive() })
    }
  }

  private func toLatest() {
    guard let latest else { return }
    FrameProbe.shared.beginJump()
    list.scrollToItem(latest.id, align: .end)
  }
}

struct MessageRow: View {
  let message: TestComment

  var body: some View {
    VStack(alignment: .leading, spacing: 2) {
      Text(message.authorId).font(.caption).foregroundStyle(.secondary)
      Text(message.text)
    }
    .frame(maxWidth: .infinity, alignment: .leading)
    .padding(.horizontal, 12)
    .padding(.vertical, 6)
    // One element with the padded row's frame: without the shape, it's
    // its texts'.
    .contentShape(.accessibility, Rectangle())
    .accessibilityElement(children: .ignore)
    .accessibilityLabel(message.text)
    .accessibilityIdentifier("row-\(message.id)")
    .background(ProbeMarker(id: message.id))
  }
}

/// A long list to page through, both ways, with a header that grows and a
/// footer, and rows to select.
struct IssuesScreen: View {
  @State private var list: IssueList
  @State private var selection: String?
  /// The header's lines of text.
  @State private var headerLines = 2

  init(backend: Backend) {
    _list = State(initialValue: backend.issueList())
  }

  var body: some View {
    VStack(spacing: 0) {
      Divider()
      issues
      Divider()
      HStack {
        Button("Grow header") { growHeader() }.accessibilityIdentifier("grow-header")
        Button("End") { list.scrollToItem(Backend.issueID(1999), align: .end) }
          .accessibilityIdentifier("end")
        Text(selection ?? "none").accessibilityIdentifier("selection")
      }
      .padding(8)
    }
    .overlay(alignment: .topTrailing) { ProbeSummary() }
    .task {
      guard FrameProbe.script == "issues" else { return }
      await ProbeScript.issues(growHeader: growHeader)
    }
  }

  /// Taller by two lines, up to 8, then back to 1.
  private func growHeader() {
    headerLines = headerLines >= 8 ? 1 : headerLines + 2
  }

  @ViewBuilder private var issues: some View {
    if Native.enabled {
      NativeVirtualList(
        list: list, selection: $selection, header: Self.header(headerLines), footer: Self.footer
      ) { item, row in
        let i = item.row
        row.show(
          id: i.id, first: i.title, second: i.description, captionFirst: false, padding: 8)
      }
    } else {
      swiftUIIssues
    }
  }

  /// The header's text: `lines` lines.
  static func header(_ lines: Int) -> String {
    (1...lines).map { "Header line \($0)" }.joined(separator: "\n")
  }

  static let footer = "The end of the issues"

  private var swiftUIIssues: some View {
    VirtualList(list, selection: $selection) { item, _ in
      VStack(alignment: .leading, spacing: 2) {
        Text(item.row.title)
        if !item.row.description.isEmpty {
          Text(item.row.description).font(.caption).foregroundStyle(.secondary)
        }
      }
      .frame(maxWidth: .infinity, alignment: .leading)
      .padding(.horizontal, 12)
      .padding(.vertical, 8)
      .contentShape(.accessibility, Rectangle())
      .accessibilityElement(children: .ignore)
      .accessibilityLabel(item.row.title)
      .accessibilityIdentifier("row-\(item.row.id)")
      .background(ProbeMarker(id: item.row.id))
    } header: {
      Accessory(text: Self.header(headerLines), id: "header")
    } footer: {
      Accessory(text: Self.footer, id: "footer")
    }
    .accessibilityIdentifier("list")
  }
}

/// The header or the footer: its text, as one element, with a probe
/// marker.
struct Accessory: View {
  let text: String
  let id: String

  var body: some View {
    Text(text)
      .font(.headline)
      .frame(maxWidth: .infinity, alignment: .leading)
      .padding(12)
      .background(.quaternary)
      .contentShape(.accessibility, Rectangle())
      .accessibilityElement(children: .ignore)
      .accessibilityLabel(text)
      .accessibilityIdentifier(id)
      .background(ProbeMarker(id: id))
  }
}
