# Changelog

## [Unreleased]

## [0.2.1](https://github.com/near/near-indexer-client/compare/v0.2.0...v0.2.1) - 2026-11-03

### Other

- rewrite the README around getting started ([#5](https://github.com/near/near-indexer-client/pull/5))

## [0.2.0](https://github.com/near/near-indexer-client/compare/v0.1.0...v0.2.0) - 2026-11-03

### Added

- [**breaking**] build against nearcore 2.14 crates and catch up in bounded, retryable chunks ([#3](https://github.com/near/near-indexer-client/pull/3))

- Depend on `near-indexer-primitives 0.38` and `near-kit 0.20`, matching nearcore
  2.14. The 0.37 primitives pin `aws-lc-rs = "=1.16.2"`, which cannot resolve
  alongside nearcore 2.14 crates.
- Re-export `near_kit` so callers construct the matching `RpcClient`.
- Add `Client::plan`, which walks ancestry without fetching blocks, and
  `Client::fetch_block`, which fetches one planned block and checks it against
  its header. Together they let a consumer catch up in bounded chunks,
  checkpointing after each, and retry a single failed request.
- Expose `Client::head`, `Client::header` and `Client::parent` for consumers that
  choose their own starting point by walking ancestry.
- Add `Error::kind` and `ErrorKind`, classifying endpoint errors by name.
- Add `RetryPolicy`, `Backoff` and `Retry`, which pace per-request retries:
  transient errors back off to 10 s, persistent ones to 60 s, and
  `DATA_UNAVAILABLE` stays transient for a grace period measured from the
  request's first failure. The client still performs no retries itself.

## [0.1.0] - 2026-09-10

- Fetch complete indexer messages by immutable block hash over NEAR JSON-RPC.
- Follow finalized or optimistic heads with consumer-owned checkpoints, bounded
  ancestry traversal, ordered rollbacks, and complete replacement batches.
- Support an explicit target hash for replay without sampling another head.
- Reject incomplete shard coverage, inconsistent ancestry, and finalized rollbacks.
- Preserve explicit `proposed_split: null` when decoding published protocol types.
- Include a checkpointed polling example and deterministic RPC fixture tests.
