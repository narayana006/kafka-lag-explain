import json
from pathlib import Path

import pytest

from kafka_lag_explain.parsers import ParseError, load_samples, parse_describe_output, sample_to_dict

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"

OFFSETS = """
GROUP  TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID       HOST        CLIENT-ID
app    orders  0          100             150             50   app-1-6f2c        /10.0.0.5   app-1
app    orders  1          -               75              -    -                 -           -
"""

STATE = """
GROUP  COORDINATOR (ID)   ASSIGNMENT-STRATEGY  STATE   #MEMBERS
app    broker-1:9092 (1)  range                Stable  2
"""


def test_parses_offsets_table():
    sample = parse_describe_output(OFFSETS)["app"]
    first, second = sample.partitions
    assert (first.topic, first.partition, first.current_offset, first.log_end_offset) == ("orders", 0, 100, 150)
    assert first.lag == 50
    assert (first.consumer_id, first.host, first.client_id) == ("app-1-6f2c", "/10.0.0.5", "app-1")
    assert second.current_offset is None and second.lag is None
    assert second.consumer_id is None and second.host is None


def test_parses_state_table_and_merges_with_offsets():
    sample = parse_describe_output(OFFSETS + STATE)["app"]
    assert sample.state == "Stable"
    assert sample.members == 2
    assert len(sample.partitions) == 2


def test_state_row_of_empty_group_has_blank_assignment_strategy():
    text = "GROUP  COORDINATOR (ID)   ASSIGNMENT-STRATEGY  STATE  #MEMBERS\napp    broker-1:9092 (1)                       Empty  0\n"
    sample = parse_describe_output(text)["app"]
    assert sample.state == "Empty"
    assert sample.members == 0


def test_no_active_members_message_sets_empty_state():
    by_group = load_samples((EXAMPLES / "no-members.txt").read_text())
    sample = by_group["billing-export"][0]
    assert sample.state == "Empty"
    assert sample.member_count == 0
    assert [p.lag for p in sample.partitions] == [7711, 7653, 7695]


def test_several_groups_in_one_output():
    text = OFFSETS + """
GROUP  TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID  HOST  CLIENT-ID
other  audit   0          5               5               0    -            -     -
"""
    groups = parse_describe_output(text)
    assert set(groups) == {"app", "other"}
    assert groups["other"].partitions[0].topic == "audit"


def test_output_without_group_column_from_old_kafka():
    text = "TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID\norders 0 10 12 2 c-1 /10.0.0.1 c\n"
    groups = parse_describe_output(text)
    assert groups["(unknown)"].partitions[0].lag == 2


def test_malformed_row_raises():
    with pytest.raises(ParseError):
        parse_describe_output("GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG\napp orders zero 1 2 1\n")


def test_text_without_any_table_raises():
    with pytest.raises(ParseError):
        load_samples("Error: Consumer group 'x' does not exist.\n")


def test_json_round_trip_and_iso_timestamps():
    by_group = load_samples((EXAMPLES / "traffic-spike.json").read_text())
    samples = by_group["clickstream-enricher"]
    assert len(samples) == 3
    assert samples[1].timestamp - samples[0].timestamp == 30
    again = load_samples(json.dumps([sample_to_dict(s) for s in samples]))["clickstream-enricher"]
    assert again == samples


def test_invalid_json_sample_raises():
    with pytest.raises(ParseError):
        load_samples('{"partitions": []}')
    with pytest.raises(ParseError):
        load_samples("{not json")
