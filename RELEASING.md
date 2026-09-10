# Releasing

Release-plz prepares a version/changelog PR after changes reach `main`. Review
and squash-merge that PR after CI passes. The release workflow validates the
actual main commit before publishing, creates its GitHub release and tag, and
attests the `.crate` artifact with GitHub/Sigstore provenance. Before attesting,
it compares the packaged bytes with the artifact served by crates.io.

## Trusted publishing

The crates.io publisher identity is GitHub repository `near/near-indexer-client`,
workflow `release-plz.yml`, environment `crates-io`. The GitHub environment permits
only `main`. Release-plz obtains a short-lived crates.io token through OIDC;
there is no long-lived registry token in GitHub secrets. `RELEASE_ENABLED=true`
enables release jobs after initial setup.

crates.io requires a maintainer token for the first publication. The initial
`0.1.0` upload is performed with `cargo publish --locked` after CI passes. Then
configure trusted publishing and dispatch `release-plz.yml` with
`attest_existing=true` to attest the same source/package bytes. Create the initial
`v0.1.0` tag and GitHub release from that exact commit after verification.
The same dispatch option can recover an attestation after a successful upload
whose attestation step failed. A byte mismatch fails the job.

The release-PR job uses `GITHUB_TOKEN` and explicitly dispatches CI for its
branch because GitHub suppresses ordinary PR-triggered workflows for that token.
Only the release-PR job has concurrency control; publication is never cancelled
by a newer queued release.

## Verification

```sh
curl -fL https://static.crates.io/crates/near-indexer-client/near-indexer-client-0.1.0.crate -o near-indexer-client-0.1.0.crate
gh attestation verify near-indexer-client-0.1.0.crate --repo near/near-indexer-client
```

Verify the crates.io artifact and docs.rs build after release. Keep the 0.37.4
protocol compatibility shim until fixed published types are selected and tested.
A nearcore source merge alone does not establish that a deployed RPC endpoint
supports the required method; verify the serving release separately.
