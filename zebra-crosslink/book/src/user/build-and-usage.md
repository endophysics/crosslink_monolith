# Building `zebra-crosslink`

These are the build and usage instructions for a generic linux system. `nix` users should check out
[`nix` Support](./nix.md).

Building `zebra-crosslink` requires [Rust](https://www.rust-lang.org/tools/install),
[libclang](https://clang.llvm.org/doxygen/group__CINDEX.html), and a C++ compiler.

## Dependencies

1. Install [`cargo` and `rustc`](https://www.rust-lang.org/tools/install).

2. Install `zebra-crosslink`'s build dependencies:

   - **libclang** is a library that might have different names depending on your
     package manager. Typical names are `libclang`, `libclang-dev`, `llvm`, or
     `llvm-dev`.
   - **clang** or another C++ compiler: `g++` (all platforms) or `Xcode` (macOS).
   - **[`protoc`](https://grpc.io/docs/protoc-installation/)**

**TODO:** We need to add prerequisites for the visualizer.

```sh
cargo build --locked zebrad
```

# Usage

```sh
cargo run
```


To connect to the current prototype testnet, see [crosslink-testnet.md](crosslink-testnet.md).
