"""Command line entry point."""

from __future__ import annotations

import argparse
import json
import sys
import time

from .analysis import SEVERITIES, Thresholds, analyze
from .model import GroupSample
from .parsers import UNKNOWN_GROUP, ParseError, load_samples, sample_to_dict
from .report import render_text


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="kafka-lag-explain",
        description="Report Kafka consumer lag for one group and explain the likely cause.",
    )
    src = p.add_argument_group("input (use --sample files or --bootstrap)")
    src.add_argument("--sample", action="append", default=[], metavar="FILE",
                     help="sample file, oldest first; repeat for a trend. JSON or the text output of "
                          "`kafka-consumer-groups --describe`. Use - for stdin.")
    src.add_argument("--bootstrap", metavar="HOST:PORT", help="sample a live cluster (needs confluent-kafka)")
    src.add_argument("--group", help="consumer group id (required with --bootstrap or when a file holds several groups)")
    src.add_argument("--interval", type=float, metavar="SECONDS",
                     help="seconds between samples. With --bootstrap: how long to wait between samples "
                          "(default 10). With --sample: the spacing of files that carry no timestamp.")
    src.add_argument("--samples", type=int, default=2, metavar="N",
                     help="number of samples to take with --bootstrap (default 2)")
    src.add_argument("-X", dest="client_config", action="append", default=[], metavar="KEY=VALUE",
                     help="extra client property for --bootstrap, e.g. -X security.protocol=SASL_SSL")
    src.add_argument("--save-samples", metavar="FILE", help="write the samples used as JSON, for later replay")
    out = p.add_argument_group("output")
    out.add_argument("--json", action="store_true", help="print the report as JSON")
    out.add_argument("--fail-on", choices=["never", "warning", "critical"], default="never",
                     help="exit with status 1 if a finding of this severity or worse is reported (default never)")
    tune = p.add_argument_group("thresholds")
    tune.add_argument("--skew-factor", type=float, default=Thresholds.skew_factor,
                      help="flag a partition whose lag exceeds this multiple of the topic median (default 5)")
    tune.add_argument("--skew-min-lag", type=int, default=Thresholds.skew_min_lag,
                      help="ignore uneven lag below this many messages (default 1000)")
    return p


def _read(path: str) -> str:
    if path == "-":
        return sys.stdin.read()
    with open(path, encoding="utf-8") as handle:
        return handle.read()


def _samples_from_files(paths: list[str], group: str | None) -> list[GroupSample]:
    samples: list[GroupSample] = []
    for path in paths:
        try:
            by_group = load_samples(_read(path))
        except OSError as exc:
            raise ParseError(f"{path}: {exc.strerror or exc}") from exc
        except ParseError as exc:
            raise ParseError(f"{path}: {exc}") from exc
        if group is None:
            if len(by_group) != 1:
                raise ParseError(f"{path} contains several groups ({', '.join(sorted(by_group))}); choose one with --group")
            group = next(iter(by_group))
        chosen = by_group.get(group) or by_group.get(UNKNOWN_GROUP)
        if not chosen:
            raise ParseError(f"{path}: group {group!r} not found (found: {', '.join(sorted(by_group))})")
        for sample in chosen:
            sample.group = group
        samples += chosen
    return samples


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if bool(args.sample) == bool(args.bootstrap):
        parser.error("provide either --sample FILE (one or more) or --bootstrap HOST:PORT")

    try:
        if args.sample:
            samples = _samples_from_files(args.sample, args.group)
        else:
            if not args.group:
                parser.error("--group is required with --bootstrap")
            if args.samples < 1:
                parser.error("--samples must be at least 1")
            from .live import fetch_sample

            config = dict(item.split("=", 1) for item in args.client_config)
            wait = args.interval if args.interval is not None else 10.0
            samples = []
            for i in range(args.samples):
                if i:
                    time.sleep(wait)
                samples.append(fetch_sample(args.bootstrap, args.group, config))
        report = analyze(samples, interval=args.interval,
                         thresholds=Thresholds(skew_factor=args.skew_factor, skew_min_lag=args.skew_min_lag))
    except ParseError as exc:
        print(f"kafka-lag-explain: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # live adapter errors: unreachable broker, unknown group, missing package
        if args.sample:
            raise
        print(f"kafka-lag-explain: {exc}", file=sys.stderr)
        return 2

    if args.save_samples:
        with open(args.save_samples, "w", encoding="utf-8") as handle:
            json.dump({"samples": [sample_to_dict(s) for s in samples]}, handle, indent=2)
            handle.write("\n")

    if args.json:
        print(json.dumps(report.to_dict(), indent=2))
    else:
        sys.stdout.write(render_text(report))

    if args.fail_on != "never" and SEVERITIES.index(report.worst_severity) >= SEVERITIES.index(args.fail_on):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
