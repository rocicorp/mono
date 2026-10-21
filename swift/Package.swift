// swift-tools-version: 6.2
import PackageDescription

let package = Package(
  name: "Zero",
  platforms: [.iOS(.v26), .macOS(.v26)],
  products: [
    .library(name: "Zero", targets: ["Zero"]),
    .library(name: "ZeroUI", targets: ["ZeroUI"]),
    .library(name: "ZeroVirtual", targets: ["ZeroVirtual"]),
    .library(name: "ZeroTesting", targets: ["ZeroTesting"]),
    .library(name: "ZeroNIOTesting", targets: ["ZeroNIOTesting"]),
  ],
  dependencies: [
    // Test-only: the WebSocket transport of the Linux end-to-end tests.
    .package(url: "https://github.com/apple/swift-nio.git", from: "2.104.0")
  ],
  targets: [
    // The system SQLite: in the SDK on Apple platforms, libsqlite3-dev on
    // Linux.
    .systemLibrary(name: "CSQLite", path: "Sources/CSQLite"),
    .target(name: "Zero", dependencies: ["CSQLite"]),
    .target(name: "ZeroUI", dependencies: ["Zero"]),
    // Virtual lists that page their rows in (`@rocicorp/zero-virtual`).
    .target(name: "ZeroVirtual", dependencies: ["Zero"]),
    .target(name: "ZeroTesting", dependencies: ["Zero"]),
    // A plain ws:// transport for end-to-end tests on Linux.
    .target(
      name: "ZeroNIOTesting",
      dependencies: [
        "Zero",
        .product(name: "NIOCore", package: "swift-nio"),
        .product(name: "NIOPosix", package: "swift-nio"),
        .product(name: "NIOHTTP1", package: "swift-nio"),
        .product(name: "NIOWebSocket", package: "swift-nio"),
      ]),
    // `swift run -c release ZeroBenchmarks`.
    .executableTarget(name: "ZeroBenchmarks", dependencies: ["Zero", "ZeroTesting"]),
    .testTarget(name: "ZeroTests", dependencies: ["Zero", "ZeroTesting"]),
    .testTarget(name: "ConformanceTests", dependencies: ["Zero", "ZeroTesting"]),
    // The Apple layer: the URLSession transport, SwiftUI, UIKit, and AppKit.
    // Empty elsewhere.
    .testTarget(
      name: "AppleTests", dependencies: ["Zero", "ZeroUI", "ZeroVirtual", "ZeroTesting"]),
    .testTarget(
      name: "ZeroVirtualTests", dependencies: ["Zero", "ZeroVirtual", "ZeroTesting"],
      exclude: ["UPSTREAM.md"]),
    .testTarget(name: "E2ETests", dependencies: ["Zero", "ZeroNIOTesting"]),
  ],
  swiftLanguageModes: [.v6]
)
