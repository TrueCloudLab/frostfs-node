<p align="center">
  <img src="./.github/logo.svg" width="500px" alt="FrostFS">
</p>

<p align="center">
  <a href="https://frostfs.info">FrostFS</a> is a decentralized distributed object storage integrated with the <a href="https://neo.org">NEO Blockchain</a>.
</p>

---
[![Report](https://goreportcard.com/badge/github.com/TrueCloudLab/frostfs-node)](https://goreportcard.com/report/github.com/TrueCloudLab/frostfs-node)
![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/TrueCloudLab/frostfs-node?sort=semver)
![License](https://img.shields.io/github/license/TrueCloudLab/frostfs-node.svg?style=popout)

# Overview

FrostFS Nodes are organized in a peer-to-peer network that takes care of storing
and distributing user's data. Any Neo user may participate in the network and
get paid for providing storage resources to other users or store their data in
FrostFS and pay a competitive price for it.

Users can reliably store object data in the FrostFS network and have a transparent
data placement process due to a decentralized architecture and flexible storage
policies. Each node is responsible for executing the storage policies that the
users select for geographical location, reliability level, number of nodes, type
of disks, capacity, etc. Thus, FrostFS gives full control over data to users.

Deep [Neo Blockchain](https://neo.org) integration allows FrostFS to be used by
dApps directly from
[NeoVM](https://docs.neo.org/docs/en-us/basic/technology/neovm.html) on the
[Smart Contract](https://docs.neo.org/docs/en-us/intro/glossary.html)
code level. This way dApps are not limited to on-chain storage and can
manipulate large amounts of data without paying a prohibitive price.

FrostFS has a native [gRPC API](https://github.com/TrueCloudLab/frostfs-api) and has
protocol gateways for popular protocols such as [AWS
S3](https://github.com/TrueCloudLab/frostfs-s3-gw),
[HTTP](https://github.com/TrueCloudLab/frostfs-http-gw),
[FUSE](https://wikipedia.org/wiki/Filesystem_in_Userspace) and
[sFTP](https://en.wikipedia.org/wiki/SSH_File_Transfer_Protocol) allowing
developers to integrate applications without rewriting their code.

# Supported platforms

Now, we only support GNU/Linux on amd64 CPUs with AVX/AVX2 instructions. More
platforms will be officially supported after release `1.0`.

The latest version of frostfs-node works with frostfs-contract
[v0.16.0](https://github.com/TrueCloudLab/frostfs-contract/releases/tag/v0.16.0).

# Building

To make all binaries you need Go 1.18+ and `make`:
```
make all
```
The resulting binaries will appear in `bin/` folder.

To make a specific binary use:
```
make bin/frostfs-<name>
```
See the list of all available commands in the `cmd` folder.

## Building with Docker

Building can also be performed in a container:
```
make docker/all                     # build all binaries
make docker/bin/frostfs-<name> # build a specific binary
```

## Docker images

To make docker images suitable for use in [frostfs-dev-env](https://github.com/TrueCloudLab/frostfs-dev-env/) use:
```
make images
```

# Contributing

Feel free to contribute to this project after reading the [contributing
guidelines](CONTRIBUTING.md).

Before starting to work on a certain topic, create a new issue first, describing
the feature/topic you are going to implement.

# Credits

FrostFS is maintained by [True Cloud Lab](https://github.com/TrueCloudLab/) with the help and
contributions from community members.

Please see [CREDITS](CREDITS.md) for details.

# License

- [GNU General Public License v3.0](LICENSE)
