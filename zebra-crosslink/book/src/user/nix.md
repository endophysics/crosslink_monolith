# `nix` Support

The `flake*` files/directories provide a `nix flake` for `zebra-crosslink`. This page assumes you
[have flake support enabled](https://nixos.wiki/wiki/Flakes) (or pass the temporary cli flags for
flake support).

## Standard flake commands

Standard flake commands all work:

- `nix build`
- `nix flake check`
- `nix develop`
- `nix flake install .`
- `nix flake install 'github:shieldedlabs/zebra-crosslink'`

The built package includes the `zebrad` binary and this book (rendered).
