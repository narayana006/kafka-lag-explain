"""Optional adapter: sample a consumer group from a live cluster with confluent-kafka.

Only read-only Admin API calls are made: DescribeGroups, OffsetFetch and ListOffsets.
"""

from __future__ import annotations

import time

from .model import GroupSample, PartitionSample


def fetch_sample(bootstrap: str, group: str, client_config: dict | None = None, timeout: float = 15.0) -> GroupSample:
    try:
        from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
        from confluent_kafka.admin import AdminClient, OffsetSpec
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise RuntimeError(
            "reading from a live cluster needs the confluent-kafka package: pip install 'kafka-lag-explain[live]'"
        ) from exc

    admin = AdminClient({"bootstrap.servers": bootstrap, **(client_config or {})})

    description = admin.describe_consumer_groups([group], request_timeout=timeout)[group].result()
    owners: dict[tuple[str, int], tuple[str, str, str]] = {}
    for member in description.members:
        assignment = member.assignment.topic_partitions if member.assignment else []
        for tp in assignment:
            owners[(tp.topic, tp.partition)] = (member.member_id, member.host, member.client_id)

    committed_future = admin.list_consumer_group_offsets(
        [ConsumerGroupTopicPartitions(group)], request_timeout=timeout
    )[group]
    committed = {
        (tp.topic, tp.partition): tp.offset
        for tp in committed_future.result().topic_partitions
        if tp.error is None and tp.offset >= 0
    }

    keys = sorted(set(owners) | set(committed))
    end_offsets: dict[tuple[str, int], int] = {}
    if keys:
        request = {TopicPartition(topic, partition): OffsetSpec.latest() for topic, partition in keys}
        for tp, future in admin.list_offsets(request, request_timeout=timeout).items():
            end_offsets[(tp.topic, tp.partition)] = future.result().offset

    state = getattr(description.state, "name", str(description.state))
    state = "".join(word.capitalize() for word in state.split("_"))  # PREPARING_REBALANCING -> PreparingRebalancing
    state = state.replace("Rebalancing", "Rebalance")  # match the names Kafka itself prints
    partitions = []
    for key in keys:
        member_id, host, client_id = owners.get(key, (None, None, None))
        partitions.append(PartitionSample(key[0], key[1], committed.get(key), end_offsets.get(key),
                                          member_id, host, client_id))
    return GroupSample(group=group, partitions=partitions, state=state, timestamp=time.time(),
                       members=len(description.members))
