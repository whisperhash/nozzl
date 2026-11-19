//! Event-consumer loop: drains envelopes from the bounded mpsc and feeds
//! them into the [`Archive`] (which under the chunked backend dispatches to
//! per-(class, kind, shard) writer tasks internally).
//!
//! The chunked archive's `put_event` is a fast operation — append to WAL,
//! push into Arrow buffer, return — so a single consumer task is sufficient
//! to keep up with the acquirer at any reasonable throughput. Heavy work
//! (Parquet serialization, `object_store` PUTs, retries) lives inside the
//! per-writer tasks owned by [`nozzl_archive::ChunkedArchive`].

use std::sync::Arc;

use nozzl_acquirer::EventEnvelope;
use nozzl_archive::{Archive, PutOutcome, SkipReason};
use nozzl_metrics::Metrics;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error};

/// Spawn a single consumer task draining `rx` into `archive`. Returns its
/// `JoinHandle` so the orchestrator can wait for it on shutdown.
pub fn spawn_consumer(
    rx: mpsc::Receiver<EventEnvelope>,
    archive: Arc<dyn Archive>,
    metrics: Arc<Metrics>,
) -> JoinHandle<()> {
    tokio::spawn(run_consumer(rx, archive, metrics))
}

async fn run_consumer(
    mut rx: mpsc::Receiver<EventEnvelope>,
    archive: Arc<dyn Archive>,
    metrics: Arc<Metrics>,
) {
    while let Some(envelope) = rx.recv().await {
        write_one(archive.as_ref(), metrics.as_ref(), &envelope).await;
    }
    debug!("event-consumer: channel closed; exiting");
}

async fn write_one(archive: &dyn Archive, metrics: &Metrics, envelope: &EventEnvelope) {
    let relay = envelope.relay.as_str();
    let kind_str = envelope.event.kind.as_u16().to_string();
    let slug = envelope.kind_slug.as_str();

    match archive
        .put_event(&envelope.event, &envelope.kind_slug)
        .await
    {
        Ok(PutOutcome::Written) => {
            metrics
                .events_written
                .with_label_values(&[relay, &kind_str, slug])
                .inc();
        }
        Ok(PutOutcome::AlreadyExists) => {
            metrics
                .events_deduped
                .with_label_values(&[relay, &kind_str, slug])
                .inc();
        }
        Ok(PutOutcome::Skipped {
            reason: SkipReason::NotReplaceableNorAddressable,
        }) => {
            metrics
                .events_rejected
                .with_label_values(&[relay, &kind_str, slug, "not-replaceable-nor-addressable"])
                .inc();
        }
        Ok(PutOutcome::Skipped {
            reason: SkipReason::UnsupportedKind { kind },
        }) => {
            metrics
                .events_rejected
                .with_label_values(&[relay, &kind_str, slug, "unsupported-kind"])
                .inc();
            debug!(
                relay = relay,
                kind = kind,
                "event skipped: kind not in archive scope"
            );
        }
        Err(e) => {
            error!(
                relay = relay,
                kind = envelope.event.kind.as_u16(),
                event_id = %envelope.event.id.to_hex(),
                error = %e,
                "archive put_event failed; dropping event"
            );
            metrics
                .events_dropped
                .with_label_values(&[relay, &kind_str, slug, "put-event-failed"])
                .inc();
        }
    }
}
