# Changelog

## 0.1.0 — unreleased

- Fetch complete indexer messages by immutable block hash over NEAR JSON-RPC.
- Follow finalized or optimistic heads with consumer-owned checkpoints, bounded
  ancestry traversal, ordered rollbacks, and complete replacement batches.
- Support an explicit target hash for replay without sampling another head.
- Reject incomplete shard coverage, inconsistent ancestry, and finalized rollbacks.
- Preserve explicit `proposed_split: null` when decoding published protocol types.
- Include a checkpointed polling example and deterministic RPC fixture tests.
