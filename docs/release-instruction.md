# Release instructions

## Pre-release checks

These should run successfully:

* `make all`;
* `make test`;
* `make lint` (should not change any files);
* `make fmts` (should not change any files);
* `go mod tidy` (should not change any files);
* integration tests in [frostfs-devenv](https://github.com/TrueCloudLab/frostfs-devenv).

## Make release commit

Use `vX.Y.Z` tag for releases and `vX.Y.Z-rc.N` for release candidates
following the [semantic versioning](https://semver.org/) standard.

Determine the revision number for the release:

```shell
$ export FROSTFS_REVISION=X.Y.Z[-rc.N]
$ export FROSTFS_TAG_PREFIX=v
```

Double-check the number:

```shell
$ echo ${FROSTFS_REVISION}
```

Create release branch from the main branch of the origin repository:

```shell
$ git checkout -b release/${FROSTFS_TAG_PREFIX}${FROSTFS_REVISION}
```

### Update versions

Write new revision number into the root `VERSION` file:

```shell
$ echo ${FROSTFS_TAG_PREFIX}${FROSTFS_REVISION} > VERSION
```

Update version in Debian package changelog file
```
$ cat debian/changelog
```

Update the supported version of `TrueCloudLab/frostfs-contract` module in root
`README.md` if needed.

### Writing changelog

Add an entry to the `CHANGELOG.md` following the style established there.

* copy `Unreleased` section (next steps relate to section below `Unreleased`)
* replace `Unreleased` link with the new revision number
* update `Unreleased...new` and `new...old` diff-links at the bottom of the file
* add optional codename and release date in the heading
* remove all empty sections such as `Added`, `Removed`, etc.
* make sure all changes have references to GitHub issues in `#123` format (if possible)
* clean up all `Unreleased` sections and leave them empty

### Make release commit

Stage changed files for commit using `git add`. Commit the changes:

```shell
$ git commit -s -m 'Release '${FROSTFS_TAG_PREFIX}${FROSTFS_REVISION}
```

### Open pull request

Push release branch:

```shell
$ git push <remote> release/${FROSTFS_TAG_PREFIX}${FROSTFS_REVISION}
```

Open pull request to the main branch of the origin repository so that the
maintainers check the changes. Remove release branch after the merge.

## Tag the release

Pull the main branch with release commit created in previous step. Tag the commit
with PGP signature.

```shell
$ git checkout master && git pull
$ git tag -s ${FROSTFS_TAG_PREFIX}${FROSTFS_REVISION}
```

## Push the release tag

```shell
$ git push origin ${FROSTFS_TAG_PREFIX}${FROSTFS_REVISION}
```

## Post-release

### Prepare and push images to a Docker Hub (if not automated)

Create Docker images for all applications and push them into Docker Hub
(requires [organization](https://hub.docker.com/u/truecloudlab) privileges)

```shell
$ git checkout ${FROSTFS_TAG_PREFIX}${FROSTFS_REVISION}
$ make images
$ docker push truecloudlab/frostfs-storage:${FROSTFS_REVISION}
$ docker push truecloudlab/frostfs-storage-testnet:${FROSTFS_REVISION}
$ docker push truecloudlab/frostfs-ir:${FROSTFS_REVISION}
$ docker push truecloudlab/frostfs-cli:${FROSTFS_REVISION}
$ docker push truecloudlab/frostfs-adm:${FROSTFS_REVISION}
```

### Make a proper GitHub release (if not automated)

Edit an automatically-created release on GitHub, copy things from `CHANGELOG.md`.
Build and tar release binaries with `make prepare-release`, attach them to
the release. Publish the release.

### Update FrostFS Developer Environment

Prepare pull-request in [frostfs-devenv](https://github.com/TrueCloudLab/frostfs-devenv)
with new versions.

### Close GitHub milestone

Look up GitHub [milestones](https://github.com/TrueCloudLab/frostfs-node/milestones) and close the release one if exists.

### Rebuild FrostFS LOCODE database

If new release contains LOCODE-related changes, rebuild FrostFS LOCODE database via FrostFS CLI

```shell
$ frostfs-cli util locode generate ...
```
