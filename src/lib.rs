//! Pull complete NEAR indexer messages over HTTP without running a node.
//!
//! [`Client::poll`] returns ordered rollback/application work. Process the whole
//! [`Update`] and persist its checkpoint in the same transaction as consumer
//! state. The client never advances a cursor or acknowledges queue delivery.

use std::collections::BTreeSet;
use std::time::Duration;

pub use near_indexer_primitives::{CryptoHash, StreamerMessage};
/// The `near-kit` version whose [`RpcClient`] [`Client::new`] takes and whose
/// [`RpcError`] [`Error::Rpc`] carries, so callers need not pin it separately.
pub use near_kit;
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
    /// Finality requested when sampling the head; explicit targets are optimistic.
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

/// The headers of an [`Update`], before any block is fetched.
///
/// Fetch [`Self::apply`] with [`Client::fetch_block`] in chunks of any size and
/// persist each chunk's last header as the checkpoint, so a long catch-up keeps
/// bounded memory and a failed fetch repeats only that block.
///
/// ```no_run
/// use near_indexer_client::{Checkpoint, Client, Error};
///
/// async fn catch_up(client: &Client, mut saved: Checkpoint) -> Result<Checkpoint, Error> {
///     let head = client.head().await?;
///     let plan = client.plan(Some(&saved), head).await?;
///     // An optimistic consumer undoes `plan.rollback` first.
///     for chunk in plan.apply.chunks(32) {
///         for header in chunk {
///             // Retry a failed fetch on its own, paced by a `Backoff`.
///             let _block = client.fetch_block(header).await?;
///         }
///         if let Some(last) = chunk.last() {
///             // Persist the chunk's effects and this checkpoint together.
///             saved = last.checkpoint();
///         }
///     }
///     Ok(saved)
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Plan {
    /// Sampled head; its checkpoint follows the last applied block.
    pub head: Header,
    /// Replaced ancestry to undo, newest first, as in [`Update::rollback`].
    pub rollback: Vec<Header>,
    /// Blocks to fetch and apply, oldest first, ending with [`Self::head`]
    /// unless the head is the checkpoint itself.
    pub apply: Vec<Header>,
}

/// Which head to follow. Optimistic consumers must implement rollback.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
    Rpc(#[from] Box<RpcError>),
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

impl From<RpcError> for Error {
    fn from(error: RpcError) -> Self {
        Self::Rpc(Box::new(error))
    }
}

impl Error {
    /// Classify the error, e.g. to choose a retry delay with [`Backoff`] or to
    /// label it in metrics.
    pub fn kind(&self) -> ErrorKind {
        let rpc = match self {
            Self::InvalidData(_) => return ErrorKind::InvalidData,
            Self::AncestryLimit(_) => return ErrorKind::AncestryLimit,
            Self::FinalityViolation => return ErrorKind::FinalityViolation,
            Self::Rpc(rpc) => rpc.as_ref(),
        };
        let name = match rpc {
            RpcError::Rpc { data, .. } => data
                .as_ref()
                .and_then(|data| data.get("name"))
                .and_then(Value::as_str),
            _ => None,
        };
        match name {
            Some("BUSY") => ErrorKind::Busy,
            Some("TIMEOUT_ERROR") => ErrorKind::Timeout,
            Some("DATA_UNAVAILABLE") => ErrorKind::DataUnavailable,
            Some("INCOMPLETE_DATA") => ErrorKind::IncompleteData,
            Some("LIMIT_EXCEEDED") => ErrorKind::LimitExceeded,
            Some("UNSUPPORTED") => ErrorKind::Unsupported,
            Some(_) => ErrorKind::Rpc,
            None if rpc.is_retryable() => ErrorKind::Unavailable,
            None => ErrorKind::Rpc,
        }
    }
}

/// The class of an [`enum@Error`], from the endpoint's error name where it has one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The endpoint is at its concurrent indexer request limit.
    Busy,
    /// The endpoint timed out serving the request.
    Timeout,
    /// A transport failure or other RPC error near-kit deems retryable.
    Unavailable,
    /// The block or its execution data is not available, possibly not yet.
    DataUnavailable,
    /// The endpoint's data for the block is inconsistent or incomplete.
    IncompleteData,
    /// The block's response exceeds the endpoint's size cap.
    LimitExceeded,
    /// The endpoint is not configured to serve indexer requests.
    Unsupported,
    /// Any other RPC error.
    Rpc,
    /// See [`Error::InvalidData`].
    InvalidData,
    /// See [`Error::AncestryLimit`].
    AncestryLimit,
    /// See [`Error::FinalityViolation`].
    FinalityViolation,
}

impl ErrorKind {
    /// A stable snake_case name, suitable as a metric label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Busy => "busy",
            Self::Timeout => "timeout",
            Self::Unavailable => "unavailable",
            Self::DataUnavailable => "data_unavailable",
            Self::IncompleteData => "incomplete_data",
            Self::LimitExceeded => "limit_exceeded",
            Self::Unsupported => "unsupported",
            Self::Rpc => "rpc",
            Self::InvalidData => "invalid_data",
            Self::AncestryLimit => "ancestry_limit",
            Self::FinalityViolation => "finality_violation",
        }
    }
}

/// Whether a retry is expected to clear a failure on its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    /// The endpoint is momentarily unable to answer.
    Transient,
    /// The same request will likely keep failing until the endpoint changes;
    /// for a block error, every consumer of that block is affected.
    Persistent,
}

/// Pacing for retrying one request at a time.
///
/// The client never retries or sleeps itself; the consumer retries the failed
/// request (not the whole update) after the [`Backoff`] delay. Build the
/// [`RpcClient`] with `RetryConfig::none()` so near-kit's retries do not stack
/// underneath.
///
/// ```
/// use near_indexer_client::{Error, Failure, RetryPolicy};
/// use std::time::Duration;
///
/// let mut backoff = RetryPolicy::default().backoff();
/// let error = Error::InvalidData("broken ancestry");
/// let retry = backoff.next(&error, Duration::ZERO);
/// assert_eq!(retry.failure, Failure::Persistent);
/// assert_eq!(retry.delay, Duration::from_millis(500));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Delay after the first failure; it doubles with each further failure.
    pub initial_delay: Duration,
    /// Cap on the delay while failures are [`Failure::Transient`].
    pub max_transient_delay: Duration,
    /// Cap on the delay once failures are [`Failure::Persistent`].
    pub max_persistent_delay: Duration,
    /// How long [`ErrorKind::DataUnavailable`] counts as transient, measured
    /// from the request's first failure. An endpoint may announce a head
    /// before it has saved that block's execution data.
    pub data_unavailable_grace: Duration,
}

impl Default for RetryPolicy {
    /// 500 ms doubling to 10 s while transient and 60 s once persistent, with
    /// a 30 s grace for unavailable data.
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_millis(500),
            max_transient_delay: Duration::from_secs(10),
            max_persistent_delay: Duration::from_secs(60),
            data_unavailable_grace: Duration::from_secs(30),
        }
    }
}

impl RetryPolicy {
    /// Classify an error of `kind` from a request failing for `failing_for`
    /// since its first failure.
    pub fn classify(&self, kind: ErrorKind, failing_for: Duration) -> Failure {
        match kind {
            ErrorKind::Busy | ErrorKind::Timeout | ErrorKind::Unavailable => Failure::Transient,
            ErrorKind::DataUnavailable if failing_for < self.data_unavailable_grace => {
                Failure::Transient
            }
            _ => Failure::Persistent,
        }
    }

    /// Start pacing the retries of one failing request.
    pub fn backoff(self) -> Backoff {
        Backoff {
            delay: self.initial_delay,
            policy: self,
        }
    }
}

/// Retry delays for one failing request; start a new one per request.
#[derive(Clone, Debug)]
pub struct Backoff {
    policy: RetryPolicy,
    delay: Duration,
}

/// How to retry after one failed attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Retry {
    /// The class of the error.
    pub kind: ErrorKind,
    /// Whether the error is expected to clear on its own.
    pub failure: Failure,
    /// How long to wait before the next attempt.
    pub delay: Duration,
}

impl Backoff {
    /// Classify `error`, which the request has been failing with for
    /// `failing_for` since its first failure, and return the delay before the
    /// next attempt.
    pub fn next(&mut self, error: &Error, failing_for: Duration) -> Retry {
        let kind = error.kind();
        let failure = self.policy.classify(kind, failing_for);
        Retry {
            kind,
            failure,
            delay: self.next_delay(failure),
        }
    }

    /// The delay before the next attempt after a failure the consumer
    /// classified itself, such as one in its own processing.
    pub fn next_delay(&mut self, failure: Failure) -> Duration {
        let cap = match failure {
            Failure::Transient => self.policy.max_transient_delay,
            Failure::Persistent => self.policy.max_persistent_delay,
        };
        let delay = self.delay.min(cap);
        self.delay = self.delay.saturating_mul(2).min(
            self.policy
                .max_persistent_delay
                .max(self.policy.max_transient_delay),
        );
        delay
    }
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

    /// Fetch the block `header` describes, rejecting a payload whose height,
    /// parent or finality marker differs from it. Coverage rules match [`Self::fetch`].
    pub async fn fetch_block(&self, header: &Header) -> Result<IndexerBlock, Error> {
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
        Ok(block)
    }

    /// Sample the head header at the configured finality.
    pub async fn head(&self) -> Result<Header, Error> {
        self.block_header(json!({"finality": self.finality})).await
    }

    /// Read the header of one exact block hash.
    pub async fn header(&self, hash: CryptoHash) -> Result<Header, Error> {
        let header = self.block_header(json!({"block_id": hash})).await?;
        if header.hash != hash {
            return Err(Error::InvalidData(
                "requested hash differs from returned header",
            ));
        }
        Ok(header)
    }

    /// Read the parent header of `child`, rejecting a parent that does not
    /// match `child.prev_hash` or is not below it.
    pub async fn parent(&self, child: &Header) -> Result<Header, Error> {
        let parent = self
            .block_header(json!({"block_id": child.prev_hash}))
            .await?;
        if parent.hash != child.prev_hash || parent.height >= child.height {
            return Err(Error::InvalidData("broken ancestry or no common ancestor"));
        }
        Ok(parent)
    }

    /// Walk hash ancestry from `head` back to `checkpoint` without fetching any
    /// block. Pass a head sampled by [`Self::head`]: [`Finality::Final`] rejects
    /// any rollback. Ancestry and checkpoint rules match [`Self::poll`].
    pub async fn plan(&self, checkpoint: Option<&Checkpoint>, head: Header) -> Result<Plan, Error> {
        self.plan_with(checkpoint, head, self.finality).await
    }

    /// Sample a head, walk hash ancestry, then fetch the complete update.
    ///
    /// `None` starts with the current head only. A saved checkpoint resumes
    /// exclusively after that block. An unchanged head returns an empty update.
    /// Missing/pruned history, failed fetches, and excessive catch-up depth fail
    /// closed. Retain the old checkpoint on failure and retry at your own pace.
    /// For bounded memory during a long catch-up, use [`Self::plan`] instead.
    pub async fn poll(&self, checkpoint: Option<&Checkpoint>) -> Result<Update, Error> {
        let head = self.head().await?;
        self.update(checkpoint, head, self.finality).await
    }

    /// Build an update to an exact target without resampling the moving head.
    ///
    /// Explicit targets carry [`Finality::Optimistic`]: a hash alone does not
    /// establish finality. Ancestry, coverage, and checkpoint rules match [`Self::poll`].
    pub async fn update_to(
        &self,
        checkpoint: Option<&Checkpoint>,
        target: CryptoHash,
    ) -> Result<Update, Error> {
        let head = self.header(target).await?;
        self.update(checkpoint, head, Finality::Optimistic).await
    }

    async fn update(
        &self,
        checkpoint: Option<&Checkpoint>,
        head: Header,
        finality: Finality,
    ) -> Result<Update, Error> {
        let plan = self.plan_with(checkpoint, head, finality).await?;
        let mut blocks = Vec::with_capacity(plan.apply.len());
        for header in &plan.apply {
            blocks.push(self.fetch_block(header).await?);
        }
        Ok(Update {
            finality,
            head,
            rollback: plan.rollback,
            blocks,
            next_checkpoint: head.checkpoint(),
        })
    }

    async fn plan_with(
        &self,
        checkpoint: Option<&Checkpoint>,
        head: Header,
        finality: Finality,
    ) -> Result<Plan, Error> {
        if self.max_ancestry == 0 {
            return Err(Error::AncestryLimit(0));
        }
        let (rollback, apply) = self.walk(checkpoint, head).await?;
        if finality == Finality::Final && !rollback.is_empty() {
            return Err(Error::FinalityViolation);
        }
        Ok(Plan {
            head,
            rollback,
            apply,
        })
    }

    async fn block_header(&self, params: Value) -> Result<Header, Error> {
        #[derive(Deserialize)]
        struct Block {
            header: Header,
        }
        Ok(self.rpc.call::<_, Block>("block", params).await?.header)
    }

    async fn walk(
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
