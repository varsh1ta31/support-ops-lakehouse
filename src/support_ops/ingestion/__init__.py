"""Batch and streaming ingestion entry points."""

from support_ops.ingestion.batch import BatchIngestionResult, ingest_batch

__all__ = ["BatchIngestionResult", "ingest_batch"]
