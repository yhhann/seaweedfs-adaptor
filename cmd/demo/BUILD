package(default_visibility = ["//seaweedfs-adaptor:__subpackages__"])

load("@io_bazel_rules_go//go:def.bzl", "go_binary")

go_binary(
    name = "weedfs",
    srcs = glob(
        ["*.go"],
        exclude = ["*_test.go"],
    ),
    deps = [
        "//seaweedfs-adaptor/utils:go_default_library",
        "//seaweedfs-adaptor/weedfs:go_default_library",
        "//third-party-go/vendor/github.com/golang/glog:go_default_library",
    ],
)
