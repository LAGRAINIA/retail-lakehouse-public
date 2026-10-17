"""
Command line:

    python -m include.retail_generator generate --start 2026-07-01 --end 2026-07-31
    python -m include.retail_generator generate --start 2026-08-12 --incident price_x100
    python -m include.retail_generator upload            # send data/landing to the Databricks volume
    python -m include.retail_generator incidents         # list the incidents you can inject
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path

from . import config as C
from .generate import generate_batch

DEFAULT_OUT = Path("data/landing")


def main() -> None:
    parser = argparse.ArgumentParser(prog="retail_generator")
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="write landing files for one day or a range of days")
    gen.add_argument("--start", type=date.fromisoformat, default=C.EPOCH)
    gen.add_argument("--end", type=date.fromisoformat, help="last landing date (default: --start)")
    gen.add_argument("--out", type=Path, default=DEFAULT_OUT)
    gen.add_argument("--clean", action="store_true", help="no built-in data defects")
    gen.add_argument("--incident", action="append", default=[], choices=sorted(C.INCIDENTS),
                     help="inject a named incident on every generated day (repeatable)")

    up = sub.add_parser("upload", help="upload local files to the Unity Catalog landing volume")
    up.add_argument("--src", type=Path, default=DEFAULT_OUT)
    up.add_argument("--glob", default="**/*", help="only files matching this pattern, e.g. '**/*20260801*'")

    sub.add_parser("incidents", help="list the incidents you can inject")

    args = parser.parse_args()

    if args.command == "generate":
        end = args.end or args.start
        total, d = 0, args.start
        while d <= end:
            files = generate_batch(d, args.out, clean=args.clean, incidents=tuple(args.incident))
            total += len(files)
            print(f"{d}: {len(files)} files")
            d += timedelta(days=1)
        print(f"{total} files written under {args.out}/")
    elif args.command == "upload":
        from .upload import upload_files, volume_root

        files = sorted(p.relative_to(args.src).as_posix() for p in args.src.glob(args.glob) if p.is_file())
        n = upload_files(args.src, files, volume_root())
        print(f"{n} files uploaded to {volume_root()}")
    else:
        for name, description in C.INCIDENTS.items():
            print(f"{name:22} {description}")


if __name__ == "__main__":
    main()
