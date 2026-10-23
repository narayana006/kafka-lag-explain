"""Read group samples from JSON or from `kafka-consumer-groups --describe` text output."""

from __future__ import annotations

import json
from datetime import datetime

from .model import GroupSample, PartitionSample


UNKNOWN_GROUP = "(unknown)"


class ParseError(ValueError):
    """Raised when a sample cannot be read."""


def _int_or_none(value) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if text in ("", "-"):
        return None
    return int(text)


def _str_or_none(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return None if text in ("", "-") else text


def _timestamp(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()


def sample_from_dict(data: dict) -> GroupSample:
    """Build a sample from the JSON structure documented in the README."""
    try:
        partitions = [
            PartitionSample(
                topic=str(p["topic"]),
                partition=int(p["partition"]),
                current_offset=_int_or_none(p.get("current_offset")),
                log_end_offset=_int_or_none(p.get("log_end_offset")),
                consumer_id=_str_or_none(p.get("consumer_id")),
                host=_str_or_none(p.get("host")),
                client_id=_str_or_none(p.get("client_id")),
            )
            for p in data.get("partitions", [])
        ]
        return GroupSample(
            group=str(data["group"]),
            partitions=partitions,
            state=_str_or_none(data.get("state")),
            timestamp=_timestamp(data.get("timestamp")),
            members=_int_or_none(data.get("members")),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ParseError(f"invalid sample JSON: {exc!r}") from exc


def sample_to_dict(sample: GroupSample) -> dict:
    return {
        "group": sample.group,
        "state": sample.state,
        "timestamp": sample.timestamp,
        "members": sample.members,
        "partitions": [
            {
                "topic": p.topic,
                "partition": p.partition,
                "current_offset": p.current_offset,
                "log_end_offset": p.log_end_offset,
                "consumer_id": p.consumer_id,
                "host": p.host,
                "client_id": p.client_id,
            }
            for p in sample.partitions
        ],
    }


def parse_describe_output(text: str) -> dict[str, GroupSample]:
    """Parse the text printed by `kafka-consumer-groups --describe`.

    Handles the offsets table (the default, also printed with `--offsets`) and the
    `--state` table, for one or several groups, in any order. Columns are located by
    header name, so extra columns added by newer Kafka versions are ignored.
    """
    groups: dict[str, GroupSample] = {}
    header: list[str] | None = None
    last_named = UNKNOWN_GROUP  # Kafka before 2.4 printed no GROUP column

    def group(name: str) -> GroupSample:
        return groups.setdefault(name, GroupSample(group=name))

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        tokens = line.split()
        if tokens[0] == "GROUP" or (tokens[0] == "TOPIC" and "LOG-END-OFFSET" in tokens):
            header = tokens
            continue
        if line.startswith("Consumer group '") and "has no active members" in line:
            name = line.split("'")[1]
            group(name).state = group(name).state or "Empty"
            group(name).members = 0
            last_named = name
            continue
        if header is None or line.startswith(("Consumer group", "Error", "Warning", "[")):
            continue
        if "LOG-END-OFFSET" in header:
            if len(tokens) < len(header):
                raise ParseError(f"cannot parse offsets row: {raw!r}")
            row = dict(zip(header, tokens))
            try:
                group(row.get("GROUP", last_named)).partitions.append(
                    PartitionSample(
                        topic=row["TOPIC"],
                        partition=int(row["PARTITION"]),
                        current_offset=_int_or_none(row["CURRENT-OFFSET"]),
                        log_end_offset=_int_or_none(row["LOG-END-OFFSET"]),
                        consumer_id=_str_or_none(row.get("CONSUMER-ID")),
                        host=_str_or_none(row.get("HOST")),
                        client_id=_str_or_none(row.get("CLIENT-ID")),
                    )
                )
            except (KeyError, ValueError) as exc:
                raise ParseError(f"cannot parse offsets row: {raw!r}") from exc
        elif "STATE" in header:
            # ASSIGNMENT-STRATEGY is blank for an empty group, so count from the right.
            from_end = len(header) - header.index("STATE")
            if len(tokens) < from_end:
                raise ParseError(f"cannot parse state row: {raw!r}")
            entry = group(tokens[0])
            entry.state = tokens[-from_end]
            if header[-1] == "#MEMBERS" and tokens[-1].isdigit():
                entry.members = int(tokens[-1])
    return groups


def load_samples(text: str) -> dict[str, list[GroupSample]]:
    """Read every sample in a file's text, keyed by group name.

    JSON input may be one sample object, a list of samples, or an object with a
    "samples" list. Anything else is treated as `kafka-consumer-groups` output.
    """
    stripped = text.lstrip()
    if stripped.startswith(("{", "[")):
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ParseError(f"invalid JSON: {exc}") from exc
        if isinstance(data, dict) and "samples" in data:
            data = data["samples"]
        items = data if isinstance(data, list) else [data]
        result: dict[str, list[GroupSample]] = {}
        for item in items:
            sample = sample_from_dict(item)
            result.setdefault(sample.group, []).append(sample)
        return result
    parsed = parse_describe_output(text)
    if not parsed:
        raise ParseError("no consumer group rows found (expected JSON or kafka-consumer-groups --describe output)")
    return {name: [sample] for name, sample in parsed.items()}
