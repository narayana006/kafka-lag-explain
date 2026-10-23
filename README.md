# kafka-lag-explain

Report Kafka consumer lag for a consumer group and explain the likely cause in plain English: a stuck partition, a slow or missing consumer, a hot partition, a rebalance or a traffic spike.

**Status: early but usable (v0.1.0).** Lag per partition, lag trend across samples, stuck and uneven partition detection, produce versus consume rate and the written diagnosis all work today, from saved `kafka-consumer-groups` output, from JSON, or from a live cluster. Counting rebalances over time is not built yet. See [Features](#features) and [Limitations](#limitations).

A lag number tells you a consumer is behind. It does not tell you why. This tool looks at where the committed offsets and the log end offsets are, how they moved between two samples, and who owns each partition, then says what is most likely wrong and what to check next.

## Install

Python 3.10 or newer. The core has no dependencies.

```bash
git clone https://github.com/narayana006/kafka-lag-explain.git
cd kafka-lag-explain
pip install .            # analysis of saved samples only
pip install ".[live]"    # also installs confluent-kafka to sample a live cluster
```

## Quick start (no cluster needed)

`examples/` holds two captures of `kafka-consumer-groups --describe` for the same group, taken 60 seconds apart:

```bash
kafka-lag-explain --sample examples/orders-t0.txt --sample examples/orders-t60.txt --interval 60
```

```text
Consumer group: orders-service   state: unknown   members: 3
Window: 2 samples over 60s

TOPIC   PARTITION  COMMITTED  LOG-END     LAG  LAG-CHANGE  PRODUCED  CONSUMED  OWNER
orders          0    890,195  890,251      56          +4     5,989     5,985  orders-service-1
orders          1    885,961  886,010      49          +2     6,009     6,007  orders-service-1
orders          2    907,402  907,441      39          -4     6,021     6,025  orders-service-2
orders          3    866,003  877,912  11,909      +5,972     5,972         0  orders-service-2  STUCK
orders          4    899,090  899,149      59          +7     5,979     5,972  orders-service-3
orders          5    893,655  893,702      47          -8     6,007     6,015  orders-service-3

Total lag: 12,159 (+5,973 over the window)
Produce rate: 599.6 msg/s   Consume rate: 500.1 msg/s

Diagnosis
---------
[CRITICAL] Some partitions are stuck
  1 of 6 lagging partition(s) made no progress: the committed offset did not move between the first
  and last sample while 11,909 messages wait (orders-3). All of them are owned by one member
  (orders-service-2, host /10.12.4.22).
  Next steps:
    - Check consumer logs for an exception repeated at the same offset (a record that cannot be
      deserialized or processed blocks the partition until it is skipped or handled).
    - Inspect the blocking record: kafka-console-consumer --topic orders --partition 3 --offset
      866003 --max-messages 1.
    - Check for a hung processing thread or a blocked downstream call (thread dump, database, HTTP
      timeouts).
    - Rule out a sampling artefact: committed offsets only move on commit, so use a window longer
      than the commit interval (auto.commit.interval.ms defaults to 5 seconds).

[INFO] Consumers keep pace with producers
  On the partitions that are moving, producers wrote 500.1 msg/s and the group consumed 500.1 msg/s.
  Their lag is steady at 250 messages, roughly under 1s of consumption. A small steady lag is
  normal: it reflects batching and the commit interval.
```

A single sample still gives the lag table and whatever can be concluded without a trend:

```bash
kafka-lag-explain --sample examples/no-members.txt
```

```text
Consumer group: billing-export   state: Empty   members: 0
Window: single sample (no trend or rates; pass a second sample to get them)

TOPIC     PARTITION  COMMITTED  LOG-END    LAG  OWNER
invoices          0     41,200   48,911  7,711  -
invoices          1     40,877   48,530  7,653  -
invoices          2     41,512   49,207  7,695  -

Total lag: 23,059

Diagnosis
---------
[CRITICAL] The consumer group has no active members
  Group state is Empty and no partition has an owner, so nothing is consuming. Current backlog:
  23,059 messages.
  Next steps:
    - Check whether the consumer application is running: deployments, pods, crash loops, recent
      releases.
    - Check the consumers use this exact group.id and can reach and authenticate to the brokers.
    - If the group was retired on purpose, delete it (kafka-consumer-groups --delete) so it stops
      alerting.
```

`examples/traffic-spike.json` holds three JSON samples and produces the "Produce rate jumped during the window" diagnosis.

## Usage

### From saved Kafka CLI output

Capture the group twice with the tool that ships with Kafka, then pass both files, oldest first:

```bash
kafka-consumer-groups.sh --bootstrap-server broker:9092 --describe --group orders-service > t0.txt
sleep 60
kafka-consumer-groups.sh --bootstrap-server broker:9092 --describe --group orders-service > t1.txt
kafka-lag-explain --sample t0.txt --sample t1.txt --interval 60
```

- The CLI output carries no timestamp, so `--interval` tells the tool how far apart the files are. Without it you still get message counts, but no per-second rates or time estimates.
- A file may also contain the `--describe --state` table (append it to the same file); the group state and member count are then picked up too.
- If a file contains several groups (`--all-groups`), choose one with `--group`.
- `--sample -` reads standard input.
- Three or more samples let the tool compare the first and last interval, which is what separates a traffic spike from a consumer slowdown.

### From a live cluster

```bash
kafka-lag-explain --bootstrap broker:9092 --group orders-service
```

This takes two samples 10 seconds apart (`--interval`, `--samples` to change) using read-only Admin API calls (describe group, fetch committed offsets, list end offsets). Pass client properties with `-X`, for example `-X security.protocol=SASL_SSL -X sasl.mechanism=PLAIN -X sasl.username=... -X sasl.password=...`. `--save-samples file.json` stores what was read so the same analysis can be replayed later with `--sample file.json`.

### From JSON

One sample, a list of samples, or `{"samples": [...]}`:

```json
{
  "group": "orders-service",
  "state": "Stable",
  "timestamp": "2026-03-14T09:30:00Z",
  "partitions": [
    {"topic": "orders", "partition": 0, "current_offset": 884210, "log_end_offset": 884262,
     "consumer_id": "orders-service-1-4f1c", "host": "/10.12.4.21", "client_id": "orders-service-1"}
  ]
}
```

`timestamp` is epoch seconds or ISO 8601 and replaces `--interval`. `state`, `timestamp`, `members` and the three owner fields are optional; `current_offset` is `null` when the group has not committed.

### Output and exit codes

| Option | Effect |
| --- | --- |
| `--json` | Print the whole report (partitions, totals, rates, findings) as JSON. |
| `--fail-on warning\|critical` | Exit 1 when a finding of that severity or worse is present. Default `never`. |
| `--skew-factor N` | A partition is "uneven" when its lag exceeds N times the topic median (default 5). |
| `--skew-min-lag N` | Ignore uneven lag below N messages (default 1000). |

Exit status: `0` report printed, `1` only with `--fail-on`, `2` bad input or the cluster could not be read.

## What it diagnoses

| Finding | Severity | Evidence |
| --- | --- | --- |
| `NO_ACTIVE_MEMBERS` | critical if there is lag | Group state `Empty`/`Dead`, no partition has an owner. |
| `GROUP_NOT_FOUND` | warning | State `Dead` and no committed offsets at all. |
| `GROUP_STALLED` / `STUCK_PARTITIONS` | critical | A partition had lag at the start of the window and its committed offset did not move. |
| `FALLING_BEHIND` | warning | On the partitions that move, more was produced than consumed and lag grew. |
| `TRAFFIC_SPIKE` | warning | As above, and the produce rate at least doubled between the first and last interval (3+ samples). |
| `CONSUMER_SLOWDOWN` | warning | As above, and the consume rate at least halved while production stayed level (3+ samples). |
| `UNEVEN_LAG` | warning | A few partitions of a topic hold most of its lag. Says "hot key" if they also receive more traffic, "slow instance" if one member owns them all. |
| `REBALANCE` | warning | Group state is `PreparingRebalance`/`CompletingRebalance` (or `Assigning`/`Reconciling`), or partitions changed owner between samples. |
| `UNASSIGNED_PARTITIONS` | warning if they lag | The group has members but some partitions with committed offsets have no owner (typically a topic the group stopped subscribing to). |
| `OFFSETS_MOVED_BACKWARDS` | warning | A committed or log end offset decreased (offset reset, topic recreated). |
| `CATCHING_UP`, `KEEPING_PACE`, `NO_GROUP_MEMBERS_BUT_CONSUMING`, `NO_COMMITTED_OFFSET`, `HEALTHY` | info | Context: time to clear the backlog, steady lag, manual `assign()` consumers, partitions with no commit. |

Lag is `log end offset - committed offset`, the same definition `kafka-consumer-groups` uses.

### As a library

```python
from kafka_lag_explain import GroupSample, PartitionSample, analyze

before = GroupSample("g", [PartitionSample("t", 0, current_offset=100, log_end_offset=900, consumer_id="c-1")], timestamp=0)
after = GroupSample("g", [PartitionSample("t", 0, current_offset=100, log_end_offset=1500, consumer_id="c-1")], timestamp=60)
report = analyze([before, after])
print(report.total_lag, [f.code for f in report.findings])  # 1400 ['GROUP_STALLED']
```

## Features

- [x] Lag per group, topic and partition
- [x] Lag trend over a sampling window (two or more samples)
- [x] Detects stuck partitions and uneven partitions
- [x] Flags a rebalance in progress and partitions that changed owner between samples
- [ ] Counts rebalances over time to detect a rebalance storm
- [x] Compares produce rate with consume rate
- [x] Plain-English diagnosis with next steps
- [x] Input from `kafka-consumer-groups --describe` text, JSON, or a live cluster

## Limitations

- **Committed offsets, not positions.** The tool sees what the group has committed. A consumer that processes but commits rarely looks stuck. Use a window clearly longer than the commit interval before trusting a "stuck" finding.
- **Two samples cannot tell a traffic spike from a slow consumer.** With two samples the tool reports that consumers are slower than producers and says so; the spike and slowdown findings need at least three.
- **No rebalance history.** A rebalance is only noticed if a sample catches the group mid-rebalance or partition owners differ between samples. Generation counts and rebalance frequency are not tracked.
- **Offsets are counted, not messages.** On compacted topics and topics with transactions, offset differences overstate the number of records, so lag and rates are upper bounds.
- **One group per run.** Share groups (KIP-932) are not supported.
- **Live mode** was tested against a single-node Apache Kafka 4.4.0 broker (KRaft) with confluent-kafka 2.16 over PLAINTEXT. Secured clusters should work through `-X` client properties but have not been tested. The automated tests cover the analysis, the parsers and the command line with fixture data; they do not start a broker.
- The text parser was checked against output of `kafka-consumer-groups.sh` from Kafka 4.4.0 and locates columns by header name. Very old releases that print a different layout may not parse.

## Contributing

Bug reports with the sample files that produced a wrong diagnosis are the most useful contribution: a sample is small and makes a good test case. Pull requests are welcome; run `pip install -e ".[test]" && pytest` before sending one.

Built and maintained by [Narayana Vemburaj](https://github.com/narayana006).

## License

[MIT](LICENSE)
