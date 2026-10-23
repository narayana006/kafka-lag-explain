import pytest

from kafka_lag_explain import GroupSample, PartitionSample, Thresholds, analyze


def sample(offsets, ts=None, state="Stable", owners=None, group="g", topic="t"):
    """offsets: list of (committed, log_end) per partition; owners: member id per partition."""
    parts = []
    for i, (committed, end) in enumerate(offsets):
        owner = owners[i] if owners else f"member-{i % 2}"
        parts.append(PartitionSample(topic, i, committed, end, owner, f"/10.0.0.{i % 2}", owner))
    return GroupSample(group=group, partitions=parts, state=state, timestamp=ts)


def codes(report):
    return [f.code for f in report.findings]


def test_single_sample_reports_lag_per_partition_and_total():
    report = analyze([sample([(90, 100), (50, 50), (None, 30)])])
    assert [p.lag for p in report.partitions] == [10, 0, None]
    assert report.total_lag == 10
    assert report.window_seconds is None and report.produce_rate is None
    assert "NO_COMMITTED_OFFSET" in codes(report)


def test_lag_is_never_negative():
    # The committed offset can briefly be ahead of a stale log end offset reading.
    assert analyze([sample([(105, 100)])]).partitions[0].lag == 0


def test_healthy_group():
    report = analyze([sample([(100, 100), (200, 200)], ts=0), sample([(160, 160), (260, 260)], ts=60)])
    assert codes(report) == ["HEALTHY"]
    assert report.worst_severity == "info"
    assert report.produce_rate == report.consume_rate == 2.0


def test_stuck_partition_detected_while_others_move():
    before = sample([(1000, 1010), (500, 2500), (1000, 1005)], ts=0, owners=["a", "b", "c"])
    after = sample([(1600, 1612), (500, 3100), (1600, 1603)], ts=60, owners=["a", "b", "c"])
    report = analyze([before, after])
    stuck = [p.name for p in report.partitions if p.stuck]
    assert stuck == ["t-1"]
    finding = report.findings[0]
    assert finding.code == "STUCK_PARTITIONS" and finding.severity == "critical"
    assert finding.partitions == ["t-1"]
    assert "--partition 1 --offset 500" in " ".join(finding.next_steps)
    # the remaining partitions keep pace, so the group is not reported as falling behind
    assert "FALLING_BEHIND" not in codes(report)
    assert "UNEVEN_LAG" not in codes(report)


def test_whole_group_stalled():
    report = analyze([sample([(10, 500), (10, 400)], ts=0), sample([(10, 900), (10, 800)], ts=30)])
    assert report.findings[0].code == "GROUP_STALLED"
    assert report.consume_rate == 0


def test_partition_with_no_backlog_at_start_is_not_stuck():
    # Messages arrived during the window and are not committed yet: not enough evidence.
    report = analyze([sample([(100, 100), (100, 100)], ts=0), sample([(100, 103), (150, 150)], ts=10)])
    assert not any(p.stuck for p in report.partitions)


def test_no_active_members_is_critical_when_there_is_lag():
    before = sample([(10, 100)], ts=0, state="Empty", owners=[None])
    after = sample([(10, 400)], ts=60, state="Empty", owners=[None])
    report = analyze([before, after])
    assert codes(report) == ["NO_ACTIVE_MEMBERS"]
    assert report.findings[0].severity == "critical"
    assert "300 messages" in report.findings[0].detail
    assert report.members == 0


def test_no_members_without_lag_is_informational():
    report = analyze([sample([(100, 100)], state="Empty", owners=[None])])
    assert report.findings[0].code == "NO_ACTIVE_MEMBERS"
    assert report.worst_severity == "info"


def test_manual_assignment_consumer_is_not_flagged_as_dead():
    before = sample([(100, 150)], ts=0, state="Empty", owners=[None])
    after = sample([(700, 760)], ts=60, state="Empty", owners=[None])
    report = analyze([before, after])
    assert "NO_GROUP_MEMBERS_BUT_CONSUMING" in codes(report)
    assert "NO_ACTIVE_MEMBERS" not in codes(report)
    assert report.worst_severity == "info"


def test_falling_behind_with_two_samples():
    report = analyze([sample([(0, 100), (0, 100)], ts=0), sample([(600, 1300), (600, 1300)], ts=60)])
    finding = next(f for f in report.findings if f.code == "FALLING_BEHIND")
    assert finding.severity == "warning"
    assert "20.0 msg/s" in finding.detail  # consumed 1200 messages in 60 s
    assert report.lag_change == 1200
    assert report.produce_rate == pytest.approx(40.0)


def test_traffic_spike_needs_three_samples():
    s0 = sample([(0, 10), (0, 10)], ts=0)
    s1 = sample([(1000, 1010), (1000, 1010)], ts=30)
    s2 = sample([(2000, 4010), (2000, 4010)], ts=60)
    assert "TRAFFIC_SPIKE" in codes(analyze([s0, s1, s2]))
    assert "FALLING_BEHIND" in codes(analyze([s0, s2]))


def test_consumer_slowdown():
    s0 = sample([(0, 10), (0, 10)], ts=0)
    s1 = sample([(1000, 1010), (1000, 1010)], ts=30)
    s2 = sample([(1200, 2010), (1200, 2010)], ts=60)
    assert "CONSUMER_SLOWDOWN" in codes(analyze([s0, s1, s2]))


def test_catching_up_estimates_time_to_clear():
    report = analyze([sample([(0, 10000)], ts=0), sample([(4000, 11000)], ts=100)])
    finding = next(f for f in report.findings if f.code == "CATCHING_UP")
    # lag 7000, net drain 30 msg/s -> about 233 s
    assert "4 min" in finding.detail
    assert report.worst_severity == "info"


def test_uneven_lag_points_at_hot_key_when_partition_gets_more_traffic():
    before = sample([(0, 10), (0, 10), (0, 10), (0, 10)], ts=0)
    after = sample([(600, 610), (600, 612), (5000, 60000), (600, 611)], ts=60)
    finding = next(f for f in analyze([before, after]).findings if f.code == "UNEVEN_LAG")
    assert finding.partitions == ["t-2"]
    assert "hot partition key" in finding.detail


def test_uneven_lag_points_at_slow_member():
    owners = ["a", "a", "b", "b", "c", "c"]
    report = analyze([sample([(990, 1000), (995, 1000), (100, 9000), (200, 9500), (998, 1000), (999, 1000)], owners=owners)])
    finding = next(f for f in report.findings if f.code == "UNEVEN_LAG")
    assert finding.partitions == ["t-2", "t-3"]
    assert "one slow instance" in finding.detail


def test_uneven_lag_thresholds_are_configurable():
    s = sample([(0, 10), (0, 10), (0, 400)])
    assert "UNEVEN_LAG" not in codes(analyze([s]))
    assert "UNEVEN_LAG" in codes(analyze([s], thresholds=Thresholds(skew_min_lag=100)))


def test_rebalance_state_and_owner_change():
    before = sample([(0, 5), (0, 5)], ts=0, owners=["a", "b"])
    after = sample([(50, 55), (50, 55)], ts=30, owners=["a", "a"], state="PreparingRebalance")
    finding = next(f for f in analyze([before, after]).findings if f.code == "REBALANCE")
    assert finding.partitions == ["t-1"]
    assert "PreparingRebalance" in finding.detail


def test_unassigned_partition_in_group_with_members():
    report = analyze([sample([(10, 10), (10, 5000)], owners=["a", None])])
    finding = next(f for f in report.findings if f.code == "UNASSIGNED_PARTITIONS")
    assert finding.severity == "warning" and finding.partitions == ["t-1"]


def test_offsets_moving_backwards_are_reported():
    report = analyze([sample([(500, 600)], ts=0), sample([(100, 700)], ts=60)])
    assert "OFFSETS_MOVED_BACKWARDS" in codes(report)


def test_interval_is_used_when_samples_have_no_timestamps():
    report = analyze([sample([(0, 0)]), sample([(300, 300)]), sample([(600, 600)])], interval=30)
    assert report.window_seconds == 60
    assert report.consume_rate == 10
    untimed = analyze([sample([(0, 0)]), sample([(300, 300)])])
    assert untimed.window_seconds is None and untimed.consume_rate is None and untimed.consumed == 300


def test_findings_are_sorted_worst_first():
    before = sample([(0, 2000), (0, 10)], ts=0, owners=["a", "b"])
    after = sample([(0, 4000), (10, 20)], ts=60, owners=["a", "c"])
    severities = [f.severity for f in analyze([before, after]).findings]
    assert severities == sorted(severities, key=["critical", "warning", "info"].index)
    assert severities[0] == "critical"


def test_rejects_empty_input_and_mixed_groups():
    with pytest.raises(ValueError):
        analyze([])
    with pytest.raises(ValueError):
        analyze([sample([(0, 0)], group="a"), sample([(0, 0)], group="b")])


def test_dead_group_without_offsets_is_reported_as_missing():
    report = analyze([GroupSample("ghost", [], state="Dead", members=0)])
    assert codes(report) == ["GROUP_NOT_FOUND"]
    assert report.total_lag == 0
