"""Turn one or more group samples into lag figures and a diagnosis.

Everything here is pure: it takes `GroupSample` objects and returns a `Report`.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from statistics import median

from .model import NO_MEMBER_STATES, REBALANCING_STATES, GroupSample

SEVERITIES = ("info", "warning", "critical")


@dataclass
class Thresholds:
    skew_factor: float = 5.0  # a partition is skewed when its lag exceeds factor x the topic median
    skew_min_lag: int = 1000  # ...and is at least this many messages
    rate_tolerance: float = 0.05  # produce and consume rates within 5% count as keeping pace
    spike_factor: float = 2.0  # produce rate growth between first and last interval that counts as a spike


@dataclass
class Finding:
    code: str
    severity: str
    title: str
    detail: str
    next_steps: list[str] = field(default_factory=list)
    partitions: list[str] = field(default_factory=list)


@dataclass
class PartitionReport:
    topic: str
    partition: int
    current_offset: int | None
    log_end_offset: int | None
    lag: int | None
    consumer_id: str | None
    client_id: str | None
    host: str | None
    produced: int | None = None  # log end offset movement over the window
    consumed: int | None = None  # committed offset movement over the window
    lag_change: int | None = None
    stuck: bool = False

    @property
    def name(self) -> str:
        return f"{self.topic}-{self.partition}"

    @property
    def key(self) -> tuple[str, int]:
        return (self.topic, self.partition)


@dataclass
class Report:
    group: str
    state: str | None
    members: int
    samples: int
    window_seconds: float | None
    total_lag: int
    lag_change: int | None
    produced: int | None
    consumed: int | None
    produce_rate: float | None  # messages per second, None without a timed window
    consume_rate: float | None
    partitions: list[PartitionReport]
    findings: list[Finding]

    @property
    def worst_severity(self) -> str:
        return max((f.severity for f in self.findings), key=SEVERITIES.index, default="info")

    def to_dict(self) -> dict:
        data = asdict(self)
        data["worst_severity"] = self.worst_severity
        return data


def _window_seconds(first: GroupSample, last: GroupSample, interval: float | None) -> float | None:
    if first is last:
        return None
    if first.timestamp is not None and last.timestamp is not None and last.timestamp > first.timestamp:
        return last.timestamp - first.timestamp
    return interval


def _totals(first: GroupSample, last: GroupSample) -> tuple[int, int]:
    """Messages produced and consumed between two samples, over partitions present in both."""
    before = {p.key: p for p in first.partitions}
    produced = consumed = 0
    for p in last.partitions:
        b = before.get(p.key)
        if b is None:
            continue
        if p.log_end_offset is not None and b.log_end_offset is not None:
            produced += max(p.log_end_offset - b.log_end_offset, 0)
        if p.current_offset is not None and b.current_offset is not None:
            consumed += max(p.current_offset - b.current_offset, 0)
    return produced, consumed


def _names(parts: list[PartitionReport], limit: int = 6) -> str:
    names = [p.name for p in parts]
    if len(names) > limit:
        return ", ".join(names[:limit]) + f" and {len(names) - limit} more"
    return ", ".join(names)


def _fmt_duration(seconds: float) -> str:
    if seconds < 1:
        return "under 1s"
    if seconds < 90:
        return f"{seconds:.0f}s"
    if seconds < 5400:
        return f"{seconds / 60:.0f} min"
    return f"{seconds / 3600:.1f} h"


def analyze(
    samples: list[GroupSample],
    interval: float | None = None,
    thresholds: Thresholds | None = None,
) -> Report:
    """Analyze samples of one group, oldest first.

    `interval` is the number of seconds between consecutive samples; it is only used
    when the samples carry no timestamps (text output from the Kafka CLI does not).
    """
    if not samples:
        raise ValueError("at least one sample is required")
    if len({s.group for s in samples}) != 1:
        raise ValueError("all samples must belong to the same consumer group")
    t = thresholds or Thresholds()
    first, last = samples[0], samples[-1]
    windowed = len(samples) > 1
    window = _window_seconds(first, last, interval * (len(samples) - 1) if interval else None)

    before = {p.key: p for p in first.partitions}
    parts: list[PartitionReport] = []
    for p in sorted(last.partitions, key=lambda x: x.key):
        r = PartitionReport(p.topic, p.partition, p.current_offset, p.log_end_offset, p.lag,
                            p.consumer_id, p.client_id, p.host)
        b = before.get(p.key) if windowed else None
        if b is not None:
            if p.log_end_offset is not None and b.log_end_offset is not None:
                r.produced = p.log_end_offset - b.log_end_offset
            if p.current_offset is not None and b.current_offset is not None:
                r.consumed = p.current_offset - b.current_offset
            if p.lag is not None and b.lag is not None:
                r.lag_change = p.lag - b.lag
            # Stuck: messages were already waiting at the start and none were consumed since.
            r.stuck = r.consumed == 0 and bool(p.lag) and bool(b.lag)
        parts.append(r)

    total_lag = sum(p.lag or 0 for p in parts)
    produced, consumed = _totals(first, last) if windowed else (None, None)
    lag_changes = [p.lag_change for p in parts if p.lag_change is not None]
    report = Report(
        group=last.group,
        state=last.state,
        members=last.member_count,
        samples=len(samples),
        window_seconds=window,
        total_lag=total_lag,
        lag_change=sum(lag_changes) if lag_changes else None,
        produced=produced,
        consumed=consumed,
        produce_rate=produced / window if windowed and window else None,
        consume_rate=consumed / window if windowed and window else None,
        partitions=parts,
        findings=[],
    )
    findings = report.findings
    lagging = [p for p in parts if p.lag]
    stuck = [p for p in parts if p.stuck]

    # --- membership -------------------------------------------------------------
    state = (last.state or "").lower()
    no_members = state in NO_MEMBER_STATES or (not state and not last.member_ids and not last.members)
    if state == "dead" and not parts:
        findings.append(Finding(
            "GROUP_NOT_FOUND", "warning", "The consumer group does not exist",
            f"The coordinator reports group {last.group} as Dead with no committed offsets: no consumer has "
            "ever joined or committed under this group id on this cluster, or the group was deleted or expired.",
            ["Check the spelling of the group id (list groups with kafka-consumer-groups --list) and the cluster."],
        ))
        return report
    if no_members and consumed:
        findings.append(Finding(
            "NO_GROUP_MEMBERS_BUT_CONSUMING", "info",
            "Offsets advance although the group has no members",
            f"The group coordinator reports no members, yet committed offsets moved forward by {consumed:,} "
            "during the window. This is normal for consumers that use manual partition assignment "
            "(assign() instead of subscribe()) and only use the group to store offsets.",
        ))
        no_members = False
    elif no_members:
        growing = bool(produced)
        findings.append(Finding(
            "NO_ACTIVE_MEMBERS", "critical" if total_lag or growing else "info",
            "The consumer group has no active members",
            f"Group state is {last.state or 'unknown'} and no partition has an owner, so nothing is consuming. "
            + (f"Producers wrote {produced:,} messages during the window and all of it became lag. " if growing else "")
            + (f"Current backlog: {total_lag:,} messages." if total_lag else "There is no backlog right now."),
            [
                "Check whether the consumer application is running: deployments, pods, crash loops, recent releases.",
                "Check the consumers use this exact group.id and can reach and authenticate to the brokers.",
                "If the group was retired on purpose, delete it (kafka-consumer-groups --delete) so it stops alerting.",
            ],
            [p.name for p in lagging],
        ))

    rebalancing = [s.state for s in samples if (s.state or "").lower() in REBALANCING_STATES]
    owners_changed = [
        p for p in parts
        if windowed and p.key in before and before[p.key].consumer_id != p.consumer_id
        and (before[p.key].consumer_id or p.consumer_id)
    ]
    if rebalancing or owners_changed:
        detail = []
        if rebalancing:
            detail.append(f"The group was in state {rebalancing[-1]} in {len(rebalancing)} of {len(samples)} sample(s).")
        if owners_changed:
            detail.append(f"{len(owners_changed)} partition(s) changed owner between samples ({_names(owners_changed)}).")
        detail.append("Consumption pauses on revoked partitions while a rebalance runs; repeated rebalances keep lag high.")
        findings.append(Finding(
            "REBALANCE", "warning", "The group rebalanced during the observation", " ".join(detail),
            [
                "Look for members leaving and rejoining in the consumer logs (restarts, OOM kills, autoscaling).",
                "If processing a batch can exceed max.poll.interval.ms, lower max.poll.records or raise the interval.",
                "Check session.timeout.ms and heartbeat.interval.ms against network and GC pauses.",
                "Consider static membership (group.instance.id) and the cooperative-sticky assignor to soften restarts.",
            ],
            [p.name for p in owners_changed],
        ))

    unassigned = [p for p in parts if not p.consumer_id]
    if unassigned and last.member_ids and not rebalancing:
        findings.append(Finding(
            "UNASSIGNED_PARTITIONS", "warning" if any(p.lag for p in unassigned) else "info",
            "Some partitions have no owner although the group has members",
            f"{len(unassigned)} partition(s) are not assigned to any member: {_names(unassigned)}. "
            "The usual reason is that the group no longer subscribes to that topic and old committed offsets "
            "remain, so the reported lag grows forever.",
            [
                "Confirm the consumers still subscribe to these topics.",
                "If not, remove the stale offsets: kafka-consumer-groups --delete-offsets --group <group> --topic <topic>.",
            ],
            [p.name for p in unassigned],
        ))

    # --- stuck partitions -------------------------------------------------------
    if stuck and not no_members:
        everything = len(stuck) == len(lagging)
        owners = {p.consumer_id for p in stuck}
        detail = (
            f"{len(stuck)} of {len(lagging)} lagging partition(s) made no progress: the committed offset did not move "
            f"between the first and last sample while {sum(p.lag or 0 for p in stuck):,} messages wait ({_names(stuck)})."
        )
        if not everything and len(owners) == 1 and None not in owners:
            sample_part = stuck[0]
            detail += f" All of them are owned by one member ({sample_part.client_id or sample_part.consumer_id}, host {sample_part.host or 'unknown'})."
        example = stuck[0]
        findings.append(Finding(
            "GROUP_STALLED" if everything else "STUCK_PARTITIONS", "critical",
            "The whole group has stopped making progress" if everything else "Some partitions are stuck",
            detail,
            [
                "Check consumer logs for an exception repeated at the same offset (a record that cannot be "
                "deserialized or processed blocks the partition until it is skipped or handled).",
                f"Inspect the blocking record: kafka-console-consumer --topic {example.topic} --partition "
                f"{example.partition} --offset {example.current_offset} --max-messages 1.",
                "Check for a hung processing thread or a blocked downstream call (thread dump, database, HTTP timeouts).",
                "Rule out a sampling artefact: committed offsets only move on commit, so use a window longer "
                "than the commit interval (auto.commit.interval.ms defaults to 5 seconds).",
            ],
            [p.name for p in stuck],
        ))

    rewound = [p for p in parts if (p.consumed or 0) < 0 or (p.produced or 0) < 0]
    if rewound:
        findings.append(Finding(
            "OFFSETS_MOVED_BACKWARDS", "warning", "Offsets moved backwards during the window",
            f"Committed or log end offsets decreased on {_names(rewound)}. Either the group's offsets were reset "
            "(kafka-consumer-groups --reset-offsets, or auto.offset.reset after offsets expired) or the topic was "
            "deleted and recreated. Rates for these partitions are not meaningful.",
            ["Check who reset the offsets or recreated the topic before trusting the lag trend."],
            [p.name for p in rewound],
        ))

    # --- uneven partitions ------------------------------------------------------
    by_topic: dict[str, list[PartitionReport]] = {}
    for p in parts:
        if p.lag is not None:
            by_topic.setdefault(p.topic, []).append(p)
    for topic, tparts in by_topic.items():
        if len(tparts) < 2:
            continue
        mid = median(p.lag for p in tparts)
        hot = [p for p in tparts if not p.stuck and p.lag >= t.skew_min_lag and p.lag > t.skew_factor * max(mid, 1)]
        if not hot or len(hot) == len(tparts):
            continue
        share = sum(p.lag for p in hot) / max(sum(p.lag for p in tparts), 1)
        detail = (f"{_names(hot)} hold{'s' if len(hot) == 1 else ''} {share:.0%} of the lag on topic {topic} "
                  f"(median partition lag is {mid:,.0f}).")
        steps: list[str]
        produced_all = [p.produced for p in tparts if p.produced is not None]
        hot_owners = {p.consumer_id for p in hot}
        other_owners = {p.consumer_id for p in tparts if p not in hot}
        if produced_all and all(p.produced is not None and p.produced > t.skew_factor * max(median(produced_all), 1) for p in hot):
            detail += (" These partitions also received far more messages than the others during the window, "
                       "which points to a hot partition key rather than a slow consumer.")
            steps = [
                "Look at the key distribution: one key (tenant, customer, null key with a custom partitioner) dominates.",
                "Change the partitioning key or add a salt so traffic spreads; adding partitions alone does not split one key.",
            ]
        elif len(hot_owners) == 1 and None not in hot_owners and not hot_owners & other_owners and other_owners:
            owner = hot[0]
            detail += (f" All of them are owned by the same member ({owner.client_id or owner.consumer_id}, "
                       f"host {owner.host or 'unknown'}) while other members keep up, which points to one slow instance.")
            steps = [
                "Compare CPU, memory, GC pauses and network of that instance with its peers.",
                "Restart or replace the instance; its partitions are reassigned to healthy members.",
            ]
        else:
            steps = [
                "Sample twice to see whether these partitions receive more traffic (hot key) or are consumed slower.",
                "Check which member owns them and whether that member is slower than the others.",
            ]
        findings.append(Finding("UNEVEN_LAG", "warning", f"Lag on topic {topic} is concentrated on few partitions",
                                detail, steps, [p.name for p in hot]))

    # --- produce rate versus consume rate ---------------------------------------
    # Stuck partitions are reported above; judge the pace of the group on the others.
    moving = [p for p in parts if not p.stuck and p.produced is not None and p.consumed is not None]
    m_produced = sum(max(p.produced, 0) for p in moving)
    m_consumed = sum(max(p.consumed, 0) for p in moving)
    m_lag = sum(p.lag or 0 for p in moving)
    m_change = sum(p.lag_change or 0 for p in moving)
    if windowed and not no_members and m_consumed:
        unit = "msg/s" if window else "messages"
        scale = window or 1
        scope = "On the partitions that are moving, " if stuck else "During the window "
        rates = (f"{scope}producers wrote {m_produced / scale:,.1f} {unit} and the group consumed "
                 f"{m_consumed / scale:,.1f} {unit}")
        if m_produced > m_consumed * (1 + t.rate_tolerance) and m_change > 0:
            code, title, extra, steps = "FALLING_BEHIND", "Consumers are slower than producers", "", [
                "Add consumer instances (useful up to one per partition) or increase processing concurrency.",
                "Profile the processing path: slow downstream calls, synchronous commits, small max.poll.records.",
                "Compare the produce rate with normal traffic to tell a traffic spike from a consumer slowdown.",
            ]
            if len(samples) >= 3:
                p_first, c_first = _totals(samples[0], samples[1])
                p_last, c_last = _totals(samples[-2], samples[-1])
                if p_first and p_last >= t.spike_factor * p_first and c_last >= 0.8 * c_first:
                    code, title = "TRAFFIC_SPIKE", "Produce rate jumped during the window"
                    extra = (f" Production in the last interval was {p_last / p_first:.1f}x the first interval "
                             "while consumption held steady: this looks like a traffic spike.")
                    steps = [
                        "Find the source of the extra traffic (batch job, replay, retry storm, upstream incident).",
                        "If the higher rate is here to stay, add consumers or partitions; otherwise lag drains once it ends.",
                    ]
                elif c_first and c_last <= 0.5 * c_first and p_last < t.spike_factor * max(p_first, 1):
                    code, title = "CONSUMER_SLOWDOWN", "Consume rate dropped during the window"
                    extra = (f" Consumption in the last interval fell to {c_last / c_first:.0%} of the first interval "
                             "while production stayed level: the consumers slowed down.")
                    steps = [
                        "Check what changed on the consumer side: a deploy, a slow dependency, lost instances.",
                        "Check downstream latency and error rates (database, HTTP calls) at the time of the drop.",
                    ]
            growth = f" Lag grew by {m_change:,}" + (f" in {_fmt_duration(window)}." if window else ".")
            findings.append(Finding(code, "warning", title,
                                    f"{rates} ({m_consumed / m_produced:.0%} of production).{growth}{extra}", steps))
        elif m_consumed > m_produced * (1 + t.rate_tolerance) and m_lag:
            eta = ""
            if window:
                eta = f" At this pace it clears in about {_fmt_duration(m_lag / ((m_consumed - m_produced) / window))}."
            findings.append(Finding(
                "CATCHING_UP", "info", "Consumers are catching up",
                f"{rates}, so their backlog of {m_lag:,} is shrinking.{eta}",
            ))
        elif m_lag:
            behind = f", roughly {_fmt_duration(m_lag / (m_consumed / window))} of consumption" if window else ""
            findings.append(Finding(
                "KEEPING_PACE", "info", "Consumers keep pace with producers",
                f"{rates}. Their lag is steady at {m_lag:,} messages{behind}. "
                "A small steady lag is normal: it reflects batching and the commit interval.",
            ))

    uncommitted = [p for p in parts if p.current_offset is None]
    if uncommitted:
        findings.append(Finding(
            "NO_COMMITTED_OFFSET", "info", "Some partitions have no committed offset",
            f"Lag is unknown for {_names(uncommitted)} because the group has not committed an offset there yet. "
            "Where a consumer starts is decided by auto.offset.reset.",
            partitions=[p.name for p in uncommitted],
        ))

    if not any(f.severity != "info" for f in findings) and not total_lag:
        findings.append(Finding("HEALTHY", "info", "No lag", "Every partition with a committed offset is fully caught up."))

    findings.sort(key=lambda f: -SEVERITIES.index(f.severity))
    return report
