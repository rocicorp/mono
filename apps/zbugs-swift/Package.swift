// swift-tools-version: 6.2
import PackageDescription

/// zbugs on the Zero Swift client: the schema, queries, and mutators of
/// apps/zbugs/shared, ported to Swift and checked against the TS ones.
let package = Package(
  name: "ZBugs",
  platforms: [.iOS(.v26), .macOS(.v26)],
  products: [
    .library(name: "ZBugs", targets: ["ZBugs"]),
    // The app's screens; the Xcode project in App/ wraps them in an app.
    .library(name: "ZBugsUI", targets: ["ZBugsUI"]),
  ],
  dependencies: [
    .package(name: "Zero", path: "../../swift"),
    // Test-only: the backend tests connect over SwiftNIO on Linux, with
    // zero-swift's own NIOTransport (Tests/ZBugsTests/NIOTransport.swift
    // links to it).
    .package(url: "https://github.com/apple/swift-nio.git", from: "2.104.0"),
  ],
  targets: [
    .target(name: "ZBugs", dependencies: [.product(name: "Zero", package: "Zero")]),
    // SwiftUI, with a UIKit or AppKit list; empty where there is no SwiftUI.
    .target(
      name: "ZBugsUI",
      dependencies: [
        "ZBugs",
        .product(name: "Zero", package: "Zero"),
        .product(name: "ZeroUI", package: "Zero"),
        .product(name: "ZeroVirtual", package: "Zero"),
      ]),
    .testTarget(
      name: "ZBugsTests",
      dependencies: [
        "ZBugs",
        .product(name: "Zero", package: "Zero"),
        .product(name: "ZeroTesting", package: "Zero"),
        .product(name: "NIOCore", package: "swift-nio"),
        .product(name: "NIOPosix", package: "swift-nio"),
        .product(name: "NIOHTTP1", package: "swift-nio"),
        .product(name: "NIOWebSocket", package: "swift-nio"),
      ],
      exclude: ["drift-vectors.json"]),
  ],
  swiftLanguageModes: [.v6]
)
