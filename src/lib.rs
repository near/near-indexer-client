//! Pull complete NEAR indexer messages over HTTP without running a node.
//!
//! [`Client::poll`] returns ordered rollback/application work. Process the whole
//! [`Update`] and persist its checkpoint in the same transaction as consumer
//! state. The client never advances a cursor or acknowledges queue delivery.

use std::collections::BTreeSet;

pub use near_indexer_primitives::{CryptoHash, StreamerMessage};
use near_kit::rpc::{RpcClient, RpcError};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

/// The last block durably processed by a consumer, exclusive on the next poll.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Hash anchors both branch identity and restart ancestry.
    pub hash: CryptoHash,
    /// Height validates the saved hash; skipped heights are allowed.
    pub height: u64,
}

/// Minimal ancestry and finality context from the ordinary block RPC.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
pub struct Header {
    /// This block's hash.
    pub hash: CryptoHash,
    /// Parent hash, which need not have height `height - 1`.
    pub prev_hash: CryptoHash,
    /// This block's height.
    pub height: u64,
    /// Finality marker recorded in this block, not a separately sampled head.
    pub last_final_block: CryptoHash,
}
impl Header {
    /// Construct the checkpoint to persist after processing this block.
    pub fn checkpoint(&self) -> Checkpoint {
        Checkpoint {
            hash: self.hash,
            height: self.height,
        }
    }
}

/// Official indexer payload with the endpoint's explicit execution coverage.
#[derive(Clone, Debug, Serialize)]
pub struct IndexerBlock {
    /// Block, chunks, receipts, execution outcomes, and state changes.
    #[serde(flatten)]
    pub message: StreamerMessage,
    /// Shards tracked by the serving node. This client requires full coverage.
    pub tracked_shards: Vec<u64>,
}

// near-primitives 0.37.4 loses explicit null in Option<Option<TrieSplit>>.
// Preserve it until the upstream double_option fix is available in a release.
impl<'de> Deserialize<'de> for IndexerBlock {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Wire {
            #[serde(flatten)]
            message: StreamerMessage,
            tracked_shards: Vec<u64>,
        }
        let value = Value::deserialize(deserializer)?;
        let explicit_null =
            |header: &Value| header.get("proposed_split").is_some_and(Value::is_null);
        let block_nulls: Vec<bool> = value["block"]["chunks"]
            .as_array()
            .into_iter()
            .flatten()
            .map(explicit_null)
            .collect();
        let shard_nulls: Vec<bool> = value["shards"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|shard| explicit_null(&shard["chunk"]["header"]))
            .collect();
        let mut wire: Wire = serde_json::from_value(value).map_err(D::Error::custom)?;
        for (header, was_null) in wire.message.block.chunks.iter_mut().zip(block_nulls) {
            if was_null {
                header.proposed_split = Some(None);
            }
        }
        for (shard, was_null) in wire.message.shards.iter_mut().zip(shard_nulls) {
            if was_null && let Some(chunk) = &mut shard.chunk {
                chunk.header.proposed_split = Some(None);
            }
        }
        Ok(Self {
            message: wire.message,
            tracked_shards: wire.tracked_shards,
        })
    }
}

/// Work to apply atomically with its checkpoint; dropping it acknowledges nothing.
#[derive(Debug)]
pub struct Update {
    /// Finality requested when sampling the head.
    pub finality: Finality,
    /// Sampled head and its recorded finality marker.
    pub head: Header,
    /// Undo this replaced ancestry in newest-first order. It can extend before
    /// the initial starting block; consumers must tolerate absent undo entries.
    pub rollback: Vec<Header>,
    /// Process these complete messages in oldest-first order.
    pub blocks: Vec<IndexerBlock>,
    /// Persist only after successfully processing the entire update.
    pub next_checkpoint: Checkpoint,
}

/// Which head to follow. Optimistic consumers must implement rollback.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Finality {
    /// Fully finalized head; any attempted rollback is an error.
    Final,
    /// Latest head, which can be replaced even at the same height.
    Optimistic,
}

/// Every error leaves consumer-owned checkpoints untouched.
#[derive(Debug, Error)]
pub enum Error {
    /// Transport, RPC, or typed payload decoding failure.
    #[error(transparent)]
    Rpc(#[from] RpcError),
    /// Missing coverage, inconsistent hashes, or malformed ancestry.
    #[error("invalid indexer data: {0}")]
    InvalidData(&'static str),
    /// Catch-up/reorg exceeds the configured number of ancestry steps.
    #[error("ancestry limit exceeded ({0}); checkpoint not advanced")]
    AncestryLimit(usize),
    /// A finalized head would replace already processed state.
    #[error("final head would roll back processed state")]
    FinalityViolation,
}

/// Sequential, pull-based indexing; consumer speed supplies backpressure.
///
/// The provided near-kit RPC client controls transport, timeouts, and retries.
/// There is no background task, queue, database, or node runtime.
pub struct Client {
    rpc: RpcClient,
    finality: Finality,
    max_ancestry: usize,
}
impl Client {
    /// Configure head finality and a positive bound on rollback + apply blocks.
    ///
    /// Zero rejects all polls. Exceeding the bound returns an error, never a
    /// partial batch; choose a bound appropriate for the retained history.
    pub fn new(rpc: RpcClient, finality: Finality, max_ancestry: usize) -> Self {
        Self {
            rpc,
            finality,
            max_ancestry,
        }
    }

    /// Fetch one exact hash with full shard coverage. Missing data is an error.
    pub async fn fetch(&self, hash: CryptoHash) -> Result<IndexerBlock, Error> {
        let result: IndexerBlock = self
            .rpc
            .call("EXPERIMENTAL_indexer_block", json!({"block_hash":hash}))
            .await?;
        if result.message.block.header.hash != hash {
            return Err(Error::InvalidData(
                "requested hash differs from returned block",
            ));
        }
        let expected: BTreeSet<u64> = result
            .message
            .block
            .chunks
            .iter()
            .map(|c| c.shard_id.into())
            .collect();
        let tracked: BTreeSet<u64> = result.tracked_shards.iter().copied().collect();
        let actual: BTreeSet<u64> = result
            .message
            .shards
            .iter()
            .map(|s| s.shard_id.into())
            .collect();
        if expected != tracked
            || expected != actual
            || result.message.block.chunks.len() != expected.len()
            || result.message.shards.len() != expected.len()
            || result.tracked_shards.len() != expected.len()
        {
            return Err(Error::InvalidData(
                "node must supply every block shard exactly once",
            ));
        }
        Ok(result)
    }

    /// Sample a head, walk hash ancestry, then fetch the complete update.
    ///
    /// `None` starts with the current head only. A saved checkpoint resumes
    /// exclusively after that block. An unchanged head returns an empty update.
    /// Missing/pruned history, failed fetches, and excessive catch-up depth fail
    /// closed. Retain the old checkpoint on failure and retry at your own pace.
    pub async fn poll(&self, checkpoint: Option<&Checkpoint>) -> Result<Update, Error> {
        if self.max_ancestry == 0 {
            return Err(Error::AncestryLimit(0));
        }
        let head = self
            .block_header(json!({"finality": self.finality}))
            .await?;
        let (rollback, apply) = self.plan(checkpoint, head).await?;
        if self.finality == Finality::Final && !rollback.is_empty() {
            return Err(Error::FinalityViolation);
        }
        let mut blocks = Vec::with_capacity(apply.len());
        for header in apply {
            let block = self.fetch(header.hash).await?;
            let actual = &block.message.block.header;
            if actual.height != header.height
                || actual.prev_hash != header.prev_hash
                || actual.last_final_block != header.last_final_block
            {
                return Err(Error::InvalidData(
                    "indexer payload differs from ancestry header",
                ));
            }
            blocks.push(block);
        }
        Ok(Update {
            finality: self.finality,
            head,
            rollback,
            blocks,
            next_checkpoint: head.checkpoint(),
        })
    }

    async fn block_header(&self, params: Value) -> Result<Header, Error> {
        #[derive(Deserialize)]
        struct Block {
            header: Header,
        }
        Ok(self.rpc.call::<_, Block>("block", params).await?.header)
    }

    async fn parent(&self, child: &Header) -> Result<Header, Error> {
        let parent = self
            .block_header(json!({"block_id": child.prev_hash}))
            .await?;
        if parent.hash != child.prev_hash || parent.height >= child.height {
            return Err(Error::InvalidData("broken ancestry or no common ancestor"));
        }
        Ok(parent)
    }

    async fn plan(
        &self,
        checkpoint: Option<&Checkpoint>,
        head: Header,
    ) -> Result<(Vec<Header>, Vec<Header>), Error> {
        let Some(checkpoint) = checkpoint else {
            return Ok((vec![], vec![head]));
        };
        let mut old = self
            .block_header(json!({"block_id": checkpoint.hash}))
            .await?;
        if old.checkpoint() != *checkpoint {
            return Err(Error::InvalidData("checkpoint does not match node history"));
        }
        let mut new = head;
        let mut rollback = vec![];
        let mut apply = vec![];
        while old.hash != new.hash {
            if rollback.len() + apply.len() >= self.max_ancestry {
                return Err(Error::AncestryLimit(self.max_ancestry));
            }
            if old.height >= new.height {
                let previous = self.parent(&old).await?;
                rollback.push(old);
                old = previous;
            } else {
                let previous = self.parent(&new).await?;
                apply.push(new);
                new = previous;
            }
        }
        if old != new {
            return Err(Error::InvalidData(
                "same hash returned inconsistent headers",
            ));
        }
        apply.reverse();
        Ok((rollback, apply))
    }
}
