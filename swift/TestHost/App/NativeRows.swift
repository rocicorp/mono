import SwiftUI
import Zero
import ZeroVirtual

#if os(iOS)
  import UIKit
#else
  import AppKit
#endif

/// The screens' lists as UIKit or AppKit views (`-native 1`): a
/// `VirtualListView` whose rows are `NativeRowView`s.
enum Native {
  static var enabled: Bool { UserDefaults.standard.bool(forKey: "native") }
}

/// A row of two lines of text, the second in a caption style (or the
/// first, with `captionFirst`), one accessibility element like the SwiftUI
/// rows.
final class NativeRowView: PlatformView {
  #if os(iOS)
    private let first = UILabel()
    private let second = UILabel()
    private let stack: UIStackView
  #else
    private let first = NSTextField(wrappingLabelWithString: "")
    private let second = NSTextField(wrappingLabelWithString: "")
    private let stack: NSStackView
  #endif
  private var marker: Marker?
  private var padding: [NSLayoutConstraint] = []
  /// The first line's text.
  private(set) var text = ""

  init() {
    #if os(iOS)
      stack = UIStackView(arrangedSubviews: [first, second])
      stack.axis = .vertical
      stack.alignment = .fill
      for label in [first, second] {
        label.numberOfLines = 0
        label.adjustsFontForContentSizeCategory = true
      }
    #else
      stack = NSStackView(views: [first, second])
      stack.orientation = .vertical
      stack.alignment = .leading
      for label in [first, second] {
        label.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
      }
    #endif
    stack.spacing = 2
    super.init(frame: .zero)
    stack.translatesAutoresizingMaskIntoConstraints = false
    addSubview(stack)
    padding = [
      stack.topAnchor.constraint(equalTo: topAnchor),
      bottomAnchor.constraint(equalTo: stack.bottomAnchor),
    ]
    NSLayoutConstraint.activate(
      padding + [
        stack.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 12),
        stack.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -12),
      ])
    #if os(iOS)
      isAccessibilityElement = true
    #else
      setAccessibilityElement(true)
      setAccessibilityRole(.group)
    #endif
  }

  required init?(coder: NSCoder) { fatalError() }

  /// Shows a row: its id, its two lines (the second hidden when empty) and
  /// its vertical padding. Its element's identifier is `row-<id>`, or
  /// `element`.
  func show(
    id: String, first a: String, second b: String, captionFirst: Bool, padding: Double,
    element: String? = nil
  ) {
    #if os(iOS)
      first.text = a
      second.text = b
      first.font = .preferredFont(forTextStyle: captionFirst ? .caption1 : .body)
      second.font = .preferredFont(forTextStyle: captionFirst ? .body : .caption1)
      first.textColor = captionFirst ? .secondaryLabel : .label
      second.textColor = captionFirst ? .label : .secondaryLabel
      accessibilityIdentifier = element ?? "row-\(id)"
      accessibilityLabel = captionFirst ? b : a
    #else
      first.stringValue = a
      second.stringValue = b
      first.font = .preferredFont(forTextStyle: captionFirst ? .caption1 : .body)
      second.font = .preferredFont(forTextStyle: captionFirst ? .body : .caption1)
      first.textColor = captionFirst ? .secondaryLabelColor : .labelColor
      second.textColor = captionFirst ? .labelColor : .secondaryLabelColor
      setAccessibilityIdentifier(element ?? "row-\(id)")
      setAccessibilityLabel(captionFirst ? b : a)
    #endif
    text = a
    second.isHidden = b.isEmpty
    for c in self.padding { c.constant = padding }
    if FrameProbe.enabled {
      if let marker {
        marker.id = id
      } else {
        let m = Marker(id)
        m.frame = bounds
        #if os(iOS)
          m.autoresizingMask = [.flexibleWidth, .flexibleHeight]
        #else
          m.autoresizingMask = [.width, .height]
        #endif
        addSubview(m)
        marker = m
      }
    }
  }

}

/// A `VirtualListView` in a SwiftUI screen, its rows `NativeRowView`s,
/// with a header and a footer of text, and a selection, when they're
/// given. Its scroll view is the `list` element the UI tests read.
@MainActor
struct NativeVirtualList<
  C: Sendable, R: ZeroRow, K: Hashable, S: Equatable & Sendable, L: Equatable & Sendable
> {
  let list: ZeroVirtualizer<C, R, K, S, L>
  var selection: Binding<K?>?
  var header: String?
  var footer: String?
  let show: (VirtualItem<R, K>, NativeRowView) -> Void

  init(
    list: ZeroVirtualizer<C, R, K, S, L>, selection: Binding<K?>? = nil, header: String? = nil,
    footer: String? = nil, show: @escaping (VirtualItem<R, K>, NativeRowView) -> Void
  ) {
    self.list = list
    self.selection = selection
    self.header = header
    self.footer = footer
    self.show = show
  }

  func make() -> VirtualListView<C, R, K, S, L> {
    let view = VirtualListView(list) { [show] item, _, reusing in
      let row = reusing as? NativeRowView ?? NativeRowView()
      show(item, row)
      return row
    }
    #if os(iOS)
      view.scrollView.accessibilityIdentifier = "list"
    #else
      view.scrollView.setAccessibilityIdentifier("list")
    #endif
    if header != nil { view.headerView = NativeRowView() }
    if footer != nil { view.footerView = NativeRowView() }
    update(view)
    return view
  }

  func update(_ view: VirtualListView<C, R, K, S, L>) {
    var changed = false
    for (text, id, accessory) in [
      (header, "header", view.headerView), (footer, "footer", view.footerView),
    ] {
      guard let text, let row = accessory as? NativeRowView, row.text != text else { continue }
      row.show(id: id, first: text, second: "", captionFirst: false, padding: 12, element: id)
      changed = true
    }
    if changed { view.headerOrFooterHeightDidChange() }
    if let selection {
      view.selection = selection.wrappedValue
      view.onSelect = { selection.wrappedValue = $0 }
    }
  }
}

#if os(iOS)
  extension NativeVirtualList: UIViewRepresentable {
    func makeUIView(context: Context) -> VirtualListView<C, R, K, S, L> { make() }
    func updateUIView(_ view: VirtualListView<C, R, K, S, L>, context: Context) { update(view) }
  }
#else
  extension NativeVirtualList: NSViewRepresentable {
    func makeNSView(context: Context) -> VirtualListView<C, R, K, S, L> { make() }
    func updateNSView(_ view: VirtualListView<C, R, K, S, L>, context: Context) { update(view) }
  }
#endif
