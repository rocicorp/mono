#if canImport(SwiftUI)
  import SwiftUI
  import ZBugs
  import Zero
  import ZeroVirtual

  #if canImport(UIKit)
    import UIKit

    /// An issue in the UIKit list: its state's icon, its title and its
    /// subtitle.
    final class IssueCell: UIView {
      private let icon = UIImageView()
      private let title = UILabel()
      private let subtitle = UILabel()
      private let separator = UIView()

      init() {
        super.init(frame: .zero)
        title.font = .preferredFont(forTextStyle: .headline)
        title.numberOfLines = 2
        subtitle.font = .preferredFont(forTextStyle: .caption1)
        subtitle.textColor = .secondaryLabel
        icon.preferredSymbolConfiguration = .init(textStyle: .headline)
        icon.contentMode = .scaleAspectFit
        separator.backgroundColor = .separator
        for view in [icon, title, subtitle, separator] {
          view.translatesAutoresizingMaskIntoConstraints = false
          addSubview(view)
        }
        // The icon beside the title's first line, the title wrapping beside
        // it, and the subtitle below them.
        NSLayoutConstraint.activate([
          icon.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 16),
          icon.widthAnchor.constraint(equalToConstant: 22),
          icon.centerYAnchor.constraint(equalTo: title.firstBaselineAnchor, constant: -6),
          title.leadingAnchor.constraint(equalTo: icon.trailingAnchor, constant: 6),
          trailingAnchor.constraint(equalTo: title.trailingAnchor, constant: 16),
          title.topAnchor.constraint(equalTo: topAnchor, constant: 10),
          subtitle.leadingAnchor.constraint(equalTo: icon.leadingAnchor),
          trailingAnchor.constraint(equalTo: subtitle.trailingAnchor, constant: 16),
          subtitle.topAnchor.constraint(equalTo: title.bottomAnchor, constant: 4),
          bottomAnchor.constraint(equalTo: subtitle.bottomAnchor, constant: 10),
          // A plain list's separator.
          separator.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 16),
          separator.trailingAnchor.constraint(equalTo: trailingAnchor),
          separator.bottomAnchor.constraint(equalTo: bottomAnchor),
          separator.heightAnchor.constraint(equalToConstant: 1 / UIScreen.main.scale),
        ])
        isAccessibilityElement = true
        accessibilityIdentifier = "IssueCell"
        accessibilityTraits = .button
      }

      required init?(coder: NSCoder) { fatalError("init(coder:) is not supported") }

      func show(_ issue: Issue, selected: Bool) {
        icon.image = UIImage(systemName: issue.open ? "circle" : "checkmark.circle.fill")
        icon.tintColor = issue.open ? .systemGreen : .systemPurple
        title.text = issue.title
        subtitle.text = issue.subtitle
        separator.isHidden = selected
        accessibilityLabel = "\(issue.open ? "Open" : "Closed"), \(issue.title), \(issue.subtitle)"
      }
    }
  #elseif canImport(AppKit)
    import AppKit

    /// An issue in the AppKit list: its state's icon, its title and its
    /// subtitle. A table cell view, so a selected row draws them in its
    /// colors.
    final class IssueCell: NSTableCellView {
      private let icon = NSImageView()
      private let title = NSTextField(labelWithString: "")
      private let subtitle = NSTextField(labelWithString: "")

      init() {
        super.init(frame: .zero)
        icon.symbolConfiguration = .init(textStyle: .headline)
        title.font = .preferredFont(forTextStyle: .headline)
        title.lineBreakMode = .byTruncatingTail
        subtitle.font = .preferredFont(forTextStyle: .caption1)
        subtitle.textColor = .secondaryLabelColor
        subtitle.lineBreakMode = .byTruncatingTail
        // Truncated, not clipped, in a column narrower than the text.
        for label in [title, subtitle] {
          label.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        }
        imageView = icon
        textField = title
        let heading = NSStackView(views: [icon, title])
        heading.spacing = 6
        let stack = NSStackView(views: [heading, subtitle])
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 2
        stack.translatesAutoresizingMaskIntoConstraints = false
        addSubview(stack)
        NSLayoutConstraint.activate([
          stack.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 16),
          trailingAnchor.constraint(greaterThanOrEqualTo: stack.trailingAnchor, constant: 16),
          stack.topAnchor.constraint(equalTo: topAnchor, constant: 6),
          bottomAnchor.constraint(equalTo: stack.bottomAnchor, constant: 6),
        ])
        setAccessibilityElement(true)
        setAccessibilityRole(.group)
        setAccessibilityIdentifier("IssueCell")
      }

      required init?(coder: NSCoder) { fatalError("init(coder:) is not supported") }

      // The subtitle too, which isn't the cell's text field.
      override var backgroundStyle: NSView.BackgroundStyle {
        didSet { subtitle.cell?.backgroundStyle = backgroundStyle }
      }

      func show(_ issue: Issue, selected: Bool) {
        icon.image = NSImage(
          systemSymbolName: issue.open ? "circle" : "checkmark.circle.fill",
          accessibilityDescription: issue.open ? "Open" : "Closed")
        icon.contentTintColor = issue.open ? .systemGreen : .systemPurple
        title.stringValue = issue.title
        subtitle.stringValue = issue.subtitle
        setAccessibilityLabel(
          "\(issue.open ? "Open" : "Closed"), \(issue.title), \(issue.subtitle)")
      }
    }
  #endif

  typealias IssueListView = VirtualListView<
    AuthData?, Issue, String, IssueRowSort, ListContextParams
  >

  /// The issue list in UIKit or AppKit: a `VirtualListView` of `IssueCell`s.
  @MainActor
  struct NativeIssueList {
    let list: IssueVirtualizer
    /// The issue the list highlights.
    let selection: String?
    /// Opens an issue, even the highlighted one.
    let open: (String) -> Void

    func make() -> IssueListView {
      VirtualListView(list) { item, selected, reusing in
        let cell = reusing as? IssueCell ?? IssueCell()
        cell.show(item.row, selected: selected)
        return cell
      }
    }

    func update(_ view: IssueListView) {
      view.selection = selection
      view.onSelect = open
    }
  }

  #if canImport(UIKit)
    extension NativeIssueList: UIViewRepresentable {
      func makeUIView(context: Context) -> IssueListView { make() }
      func updateUIView(
        _ view: IssueListView,
        context: Context
      ) { update(view) }
    }
  #elseif canImport(AppKit)
    extension NativeIssueList: NSViewRepresentable {
      func makeNSView(context: Context) -> IssueListView { make() }
      func updateNSView(
        _ view: IssueListView,
        context: Context
      ) { update(view) }
    }
  #endif
#endif
