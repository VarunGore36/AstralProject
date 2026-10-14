#!/usr/bin/env python3
"""Expand a benchmark template config over cartesian axes into a sweep directory.

Grids stay outside the harness binary by design (see docs/benchmark-harness.md):
generate the files with this script, review the list, then sweep the directory.

Axes file: {"seed": [7, 99], "strategy": ["book-top", "trade-tally"],
"probes.0.price": ["86000", "87000"]} — dot paths, numeric segments index
into lists. Keys sort for determinism regardless of file formatting.

Usage:
    python3 ops/sweep-grid.py template.json axes.json out-dir/
"""

import copy
import itertools
import json
import pathlib
import sys


def set_path(document, path, value):
    node = document
    parts = path.split(".")
    for part in parts[:-1]:
        if isinstance(node, list):
            node = node[int(part)]
        elif part in node:
            node = node[part]
        else:
            raise KeyError(part)
    last = parts[-1]
    if isinstance(node, list):
        node[int(last)] = value
    elif last in node:
        node[last] = value
    else:
        raise KeyError(last)


def main() -> int:
    if len(sys.argv) != 4:
        print("usage: sweep-grid.py template.json axes.json out-dir/", file=sys.stderr)
        return 2
    template_path, axes_path, out_dir = (pathlib.Path(a) for a in sys.argv[1:4])
    try:
        template = json.loads(template_path.read_text())
        axes = json.loads(axes_path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        print(f"unreadable input: {error}", file=sys.stderr)
        return 1
    if not isinstance(axes, dict) or not axes:
        print("axes must be a non-empty object", file=sys.stderr)
        return 1
    for key, values in axes.items():
        if not isinstance(values, list) or not values:
            print(f"axis {key!r} needs a non-empty array", file=sys.stderr)
            return 1
    # Dry-resolve every path against the template before writing anything:
    # a typo'd axis must fail here, not mid-sweep.
    try:
        probe = copy.deepcopy(template)
        for key, values in axes.items():
            set_path(probe, key, values[0])
    except (KeyError, IndexError, ValueError) as error:
        print(f"axis {key!r} does not resolve in the template: {error}", file=sys.stderr)
        return 1

    out_dir.mkdir(parents=True, exist_ok=True)
    keys = sorted(axes)
    count = 0
    for combo in itertools.product(*(axes[key] for key in keys)):
        document = copy.deepcopy(template)
        for key, value in zip(keys, combo):
            set_path(document, key, value)
        path = out_dir / f"grid-{count:04d}.json"
        if path.exists():
            print(f"refusing to overwrite {path}", file=sys.stderr)
            return 1
        path.write_text(json.dumps(document, indent=2) + "\n")
        count += 1
    print(f"wrote {count} configs to {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
