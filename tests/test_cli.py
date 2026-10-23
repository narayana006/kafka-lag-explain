import json
from pathlib import Path

import pytest

from kafka_lag_explain.cli import main

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"
T0, T60 = str(EXAMPLES / "orders-t0.txt"), str(EXAMPLES / "orders-t60.txt")


def test_text_report_from_two_cli_dumps(capsys):
    assert main(["--sample", T0, "--sample", T60, "--interval", "60"]) == 0
    out = capsys.readouterr().out
    assert "Consumer group: orders-service" in out
    assert "Total lag: 12,159 (+5,973 over the window)" in out
    assert "[CRITICAL] Some partitions are stuck" in out
    assert "STUCK" in out


def test_json_output_and_fail_on(capsys):
    assert main(["--sample", T0, "--sample", T60, "--interval", "60", "--json", "--fail-on", "critical"]) == 1
    data = json.loads(capsys.readouterr().out)
    assert data["worst_severity"] == "critical"
    assert data["window_seconds"] == 60
    assert data["findings"][0]["code"] == "STUCK_PARTITIONS"
    assert data["partitions"][3]["stuck"] is True


def test_fail_on_warning_passes_for_info_only(capsys):
    assert main(["--sample", str(EXAMPLES / "traffic-spike.json"), "--fail-on", "critical"]) == 0
    assert main(["--sample", str(EXAMPLES / "traffic-spike.json"), "--fail-on", "warning"]) == 1


def test_save_samples_can_be_replayed(tmp_path, capsys):
    saved = tmp_path / "samples.json"
    main(["--sample", T0, "--sample", T60, "--interval", "60", "--save-samples", str(saved)])
    first = capsys.readouterr().out
    main(["--sample", str(saved), "--interval", "60"])
    assert capsys.readouterr().out == first


def test_missing_file_and_unknown_group_exit_2(capsys):
    assert main(["--sample", "does-not-exist.txt"]) == 2
    assert "does-not-exist.txt" in capsys.readouterr().err
    assert main(["--sample", T0, "--group", "nope"]) == 2
    assert "not found" in capsys.readouterr().err


def test_requires_exactly_one_input_source():
    with pytest.raises(SystemExit) as exc:
        main([])
    assert exc.value.code == 2
    with pytest.raises(SystemExit):
        main(["--sample", T0, "--bootstrap", "localhost:9092"])


def test_live_adapter_is_driven_through_fetch_sample(monkeypatch, capsys):
    from kafka_lag_explain import GroupSample, PartitionSample, live

    feed = iter([
        GroupSample("g", [PartitionSample("t", 0, 10, 500, "m", "/h", "c")], "Stable", 100.0),
        GroupSample("g", [PartitionSample("t", 0, 10, 900, "m", "/h", "c")], "Stable", 105.0),
    ])
    calls = []

    def fake(bootstrap, group, config):
        calls.append((bootstrap, group, config))
        return next(feed)

    monkeypatch.setattr(live, "fetch_sample", fake)
    assert main(["--bootstrap", "broker:9092", "--group", "g", "--interval", "0", "-X", "security.protocol=SSL"]) == 0
    assert calls == [("broker:9092", "g", {"security.protocol": "SSL"})] * 2
    assert "The whole group has stopped making progress" in capsys.readouterr().out
