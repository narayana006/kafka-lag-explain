"""Plain data structures describing one observation of a consumer group."""

from __future__ import annotations

from dataclasses import dataclass, field

# Group states during which partitions are being (re)assigned. The first two belong to
# the classic rebalance protocol, the last two to the KIP-848 consumer protocol.
REBALANCING_STATES = {"preparingrebalance", "completingrebalance", "assigning", "reconciling"}
NO_MEMBER_STATES = {"empty", "dead"}


@dataclass(frozen=True)
class PartitionSample:
    """Offsets of one topic partition as seen by one consumer group."""

    topic: str
    partition: int
    current_offset: int | None  # committed offset; None if the group never committed
    log_end_offset: int | None
    consumer_id: str | None = None  # member that owns the partition, None if unassigned
    host: str | None = None
    client_id: str | None = None

    @property
    def key(self) -> tuple[str, int]:
        return (self.topic, self.partition)

    @property
    def lag(self) -> int | None:
        if self.current_offset is None or self.log_end_offset is None:
            return None
        return max(self.log_end_offset - self.current_offset, 0)


@dataclass
class GroupSample:
    """Everything known about a group at one point in time."""

    group: str
    partitions: list[PartitionSample] = field(default_factory=list)
    state: str | None = None  # e.g. Stable, Empty, PreparingRebalance
    timestamp: float | None = None  # seconds since the epoch
    members: int | None = None

    @property
    def member_ids(self) -> set[str]:
        return {p.consumer_id for p in self.partitions if p.consumer_id}

    @property
    def member_count(self) -> int:
        return self.members if self.members is not None else len(self.member_ids)
