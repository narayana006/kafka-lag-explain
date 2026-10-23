"""Explain Kafka consumer lag from one or more offset samples."""

from .analysis import Finding, Report, Thresholds, analyze
from .model import GroupSample, PartitionSample

__version__ = "0.1.0"
__all__ = ["Finding", "GroupSample", "PartitionSample", "Report", "Thresholds", "analyze"]
