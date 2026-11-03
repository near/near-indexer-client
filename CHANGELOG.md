# Changelog

## [0.2.0] - Unreleased

- Depend on `near-indexer-primitives 0.38` and `near-kit 0.20`, matching nearcore
  2.14. The 0.37 primitives pin `aws-lc-rs = "=1.16.2"`, which cannot resolve
  alongside nearcore 2.14 crates.
- Re-export `near_kit` so callers construct the matching `RpcClient`.

## [0.1.0] - 2026-09-10

- Fetch complete indexer messages by immutable block hash over NEAR JSON-RPC.
- Follow finalized or optimistic heads with consumer-owned checkpoints, bounded
  ancestry traversal, ordered rollbacks, and complete replacement batches.
- Support an explicit target hash for replay without sampling another head.
- Reject incomplete shard coverage, inconsistent ancestry, and finalized rollbacks.
- Preserve explicit `proposed_split: null` when decoding published protocol types.
- Include a checkpointed polling example and deterministic RPC fixture tests.
