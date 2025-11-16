# Milestone 4 Workshops Guide

Welcome to the Milestone 4 Workshops / Demo guide!

To join the Milestone 4a demo network follow these steps:

1. Join the Zoom link, which is/will be posted [in the Milestone 4a Forum Thread](https://forum.zcashcommunity.com/t/crosslink-workshop-wednesday-oct-22nd-at-5pm-utc/52505).
2. Download a [`zebra-crosslink` prebuilt binary](https://github.com/ShieldedLabs/zebra-crosslink/releases) for your architecture.
  - They are named as `zebra-crosslink-viz-<architecture>` for a binary with the integrated interactive visualizer gui, which we recommend.
  - If the visualizer doesn't run on your system or you prefer a console only node, use the `zebra-crosslink-<architecture>` binary (without the `-viz` infix).
3. Configure the downloaded binary for your operating system.
  - On both linux and macOS: `chmod u+x /path/to/binary`.
  - On macOS: try to run the binary, and you should see a security warning that it could not be verified by Apple. Then, go to `System Settings -> Privacy and Security` then scroll down to the `Security` section which should show `"zebra-crosslink-viz-macos" was blocked to protect you Mac` and next to it, click `Allow anyway`.
3. Run the binary to start your node and connect to the network.

We can manually add your node to the roster. Before that your node should still be able to sync and observe the dual-chain progress.
