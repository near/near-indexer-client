# near-indexer-client

A small pull-based client for NEAR's experimental HTTP indexer endpoint. It uses
near-kit for HTTP/RPC and the official `StreamerMessage` types for blocks,
transactions, receipts, execution outcomes, and state changes.

```rust
use near_indexer_client::{Client, Finality};
use near_kit::rpc::RpcClient;

async fn example() -> Result<(), near_indexer_client::Error> {
let client = Client::new(RpcClient::new("http://127.0.0.1:3030"), Finality::Final, 1000);
let update = client.poll(None).await?; // None starts with the current head only.
for block in &update.blocks {
    println!("{}", block.message.block.header.hash);
}
// Process the whole update and persist update.next_checkpoint together.
// Pass that saved checkpoint to the next poll.
Ok(())
}
```

`fetch(hash)` reads one exact block. `poll(checkpoint)` samples a final or
optimistic head, follows parent hashes to the saved checkpoint/common ancestor,
and fetches a complete update. Heights may have gaps. A saved checkpoint is
exclusive: its block is assumed already processed. The returned update includes
its sampled head, that block's `last_final_block` marker, and requested finality,
even when there are no blocks to apply. The marker is not a separately sampled
current finalized head.

Optimistic updates contain rollback headers newest first and replacement blocks
oldest first, including when two heads have the same height. Undo those headers,
apply all blocks, and save `next_checkpoint` in one consumer transaction. Ancestry
rollback can extend before the first block originally consumed; tolerate absent
undo entries, or start from a suitable finalized checkpoint. Final mode rejects
any rollback.

The client owns no cursor, queue, database, or background task. Consumer speed
supplies backpressure. Dropping an update acknowledges nothing. A failed block
fetch returns no partial update, so retrying the old checkpoint replays the batch.
Queue insertion alone is not durable processing acknowledgment. Applications
choose when to poll again, their persistence, and their retry policy through the
provided `RpcClient`.

The configured positive ancestry limit bounds rollback plus apply blocks in one
update; zero rejects polling. Exceeding it returns `AncestryLimit`, never a
truncated batch. Missing/pruned history, inconsistent ancestry, and incomplete
shard coverage fail closed. Resuming against another endpoint requires the same
checkpoint ancestry. The client trusts the node's chain/finality assertions; it
does not perform consensus or cryptographic proof verification.

The serving node must enable `EXPERIMENTAL_indexer_block` and track every shard.
The method accepts `{"block_hash":"..."}`, not height/finality. Both reported
coverage and supplied shard IDs must exactly match the block's chunk shards.
Carried chunks can still have `chunk: null`; full tracking does not make a chunk
new. near-kit's configured retries handle transient transport/server errors;
exhaustion is returned with the checkpoint untouched. Missing data is never
silently treated as a successful empty block.

## Dependencies and compatibility

This crate uses published near-kit 0.18.0 and near-indexer-primitives 0.37.4. It
retains official protocol/crypto types but has no node, VM, store, network actor,
or embedded indexer runtime dependency. The experimental endpoint/schema must
match the serving node. Tests include captured real block and receipt payloads;
this is not a promise to preserve arbitrary future fields or variants. A small
deserialization shim preserves explicit `proposed_split: null` values that the
published protocol types otherwise conflate with an absent field. It can be
removed once the upstream fix is released.

## Example and checks

```sh
cargo run --example poll -- http://127.0.0.1:3030 final /tmp/checkpoint.json 10
cargo test
cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

Example arguments are `URL final|optimistic CHECKPOINT_FILE BLOCK_COUNT
[START_HASH]`. The optional start hash is an **exclusive** checkpoint and an
existing checkpoint file takes precedence. The count is a minimum: complete
catch-up batches are processed together. The example prints JSON lines including
ancestry, transaction counts, and receipt IDs, receivers, and execution statuses.

The stdout example flushes output before replacing its checkpoint. A crash
between those steps can duplicate output; it demonstrates at-least-once replay,
not an atomic external sink. Use one writer per checkpoint file. The example
fsyncs its temporary file but not the parent directory. Production consumers
should commit their data and checkpoint in the same database transaction.

Licensed under MIT or Apache-2.0.
