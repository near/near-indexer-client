# Releasing

Publication is a separate maintainer action. CI only checks the package; it has
no publishing credentials or release workflow.

Before the first release:

1. Create the intended `near/near-indexer-client` repository, confirm the package
   metadata points to it, and choose `main` as the default branch used by CI.
2. Confirm crates.io name availability and the intended maintainer/organization
   ownership. An absent registry entry does not reserve the name.
3. Confirm the serving nearcore build contains #16407 and test against it. Check
   actual `2.14-release` inclusion before updating the README compatibility note.
4. Keep the 0.37.4 protocol compatibility shim until a fixed published dependency
   has been verified; do not remove it merely because nearcore master is fixed.
5. Inspect `cargo package --list` for unintended files. The bundled fixture
   provenance is documented in `tests/fixtures/README.md`.
6. Replace the README prepublication note once the repository and package exist.

For each release, update the version and changelog date, review the lockfile, and
run all checks in CONTRIBUTING.md from a clean checkout. Then verify the actual
publication path without uploading:

```sh
cargo publish --dry-run --locked
```

After separate authorization to publish, a maintainer may run `cargo publish --locked`, verify the crates.io artifact and docs.rs build, and create the
matching `vVERSION` Git tag and release notes. Only describe a version as released
after those external results are verified. No publishing, remote creation, or
tag push is part of the local preparation checks.
