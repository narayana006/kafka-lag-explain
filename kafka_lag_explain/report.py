"""Render a Report as plain text."""

from __future__ import annotations

import textwrap

from .analysis import Report


def _num(value: int | None, signed: bool = False) -> str:
    if value is None:
        return "-"
    return f"{value:+,}" if signed else f"{value:,}"


def _table(rows: list[list[str]], right: set[int]) -> list[str]:
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    lines = []
    for r in rows:
        cells = [c.rjust(w) if i in right else c.ljust(w) for i, (c, w) in enumerate(zip(r, widths))]
        lines.append("  ".join(cells).rstrip())
    return lines


def render_text(report: Report, width: int = 100) -> str:
    out: list[str] = []
    out.append(f"Consumer group: {report.group}   state: {report.state or 'unknown'}   members: {report.members}")
    if report.samples == 1:
        out.append("Window: single sample (no trend or rates; pass a second sample to get them)")
    elif report.window_seconds:
        out.append(f"Window: {report.samples} samples over {round(report.window_seconds, 1):g}s")
    else:
        out.append(f"Window: {report.samples} samples, time between them unknown (pass --interval for rates)")
    out.append("")

    windowed = report.samples > 1
    header = ["TOPIC", "PARTITION", "COMMITTED", "LOG-END", "LAG"]
    if windowed:
        header += ["LAG-CHANGE", "PRODUCED", "CONSUMED"]
    header += ["OWNER", ""]
    rows = [header]
    for p in report.partitions:
        row = [p.topic, str(p.partition), _num(p.current_offset), _num(p.log_end_offset), _num(p.lag)]
        if windowed:
            row += [_num(p.lag_change, signed=True), _num(p.produced), _num(p.consumed)]
        row += [p.client_id or p.consumer_id or "-", "STUCK" if p.stuck else ""]
        rows.append(row)
    out += _table(rows, right=set(range(1, len(header) - 2)))
    out.append("")

    total = f"Total lag: {report.total_lag:,}"
    if report.lag_change is not None:
        total += f" ({report.lag_change:+,} over the window)"
    out.append(total)
    if report.produce_rate is not None and report.consume_rate is not None:
        out.append(f"Produce rate: {report.produce_rate:,.1f} msg/s   Consume rate: {report.consume_rate:,.1f} msg/s")
    elif report.produced is not None:
        out.append(f"Produced: {report.produced:,}   Consumed: {report.consumed:,}")
    out.append("")

    out.append("Diagnosis")
    out.append("---------")
    for f in report.findings:
        out.append(f"[{f.severity.upper()}] {f.title}")
        out += textwrap.wrap(f.detail, width, initial_indent="  ", subsequent_indent="  ")
        if f.next_steps:
            out.append("  Next steps:")
            for step in f.next_steps:
                out += textwrap.wrap(step, width, initial_indent="    - ", subsequent_indent="      ")
        out.append("")
    return "\n".join(out).rstrip() + "\n"
