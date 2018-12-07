package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = glob(
        ["*.go"],
        exclude = ["*_test.go"],
    ),
    deps = [
        "//seaweedfs-adaptor/utils:go_default_library",
        "//third-party-go/vendor/github.com/golang/glog:go_default_library",
    ],
)
