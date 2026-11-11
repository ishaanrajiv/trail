// swift-tools-version: 5.9
import PackageDescription

let strict: [SwiftSetting] = [.enableExperimentalFeature("StrictConcurrency")]

let package = Package(
    name: "TrailClient",
    platforms: [.iOS(.v16), .macOS(.v13)],
    products: [
        .library(name: "TrailClient", targets: ["TrailClient"]),
    ],
    targets: [
        .target(name: "TrailClient", swiftSettings: strict),
        .testTarget(name: "TrailClientTests", dependencies: ["TrailClient"], swiftSettings: strict),
    ]
)
