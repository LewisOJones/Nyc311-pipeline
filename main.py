import argparse
import time
from dateutil import parser as date_parser

from src.reader import NYC311Reader
from src.writer import SQLiteWriter
from src.runner import PipelineRunner
from src.db_utils import DBUtils


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def date_converter(s: str):
    """
    Convert various date formats into a standard ISO8601 string.
    Accepts:
      - 2024-12-01
      - 2024/12/01
      - 1 Dec 2024
      - 20241201
      - 2024-12-01T12:00:00
    """
    try:
        dt = date_parser.parse(s)
        return dt.isoformat()
    except Exception:
        raise argparse.ArgumentTypeError(f"Invalid date format: {s}")


def add_db_args(p):
    p.add_argument(
        "--db",
        type=str,
        default="data/nyc311.db",
        help="SQLite database path"
    )


def add_run_args(p):
    p.add_argument(
        "--limit",
        type=int,
        help="Max rows to fetch from NYC API"
    )
    p.add_argument(
        "--since",
        type=date_converter,
        default=None,
        help="Fetch only rows created after this timestamp"
    )

# ─────────────────────────────────────────────
# Command Implementations
# ─────────────────────────────────────────────

def cmd_run(args):
    db = DBUtils(args.db)

    # If user did not specify --since, infer from DB
    since = args.since or db.get_latest_timestamp()

    reader = NYC311Reader(limit=args.limit)
    writer = SQLiteWriter(db_path=args.db)
    runner = PipelineRunner(reader, writer)

    print(f"\nRunning ETL cycle (limit={args.limit}, since={since})")
    runner.run(since=since)

def cmd_listen(args):
    db = DBUtils(args.db)
    reader = NYC311Reader(limit=args.limit)
    writer = SQLiteWriter(db_path=args.db)
    runner = PipelineRunner(reader, writer)

    interval = args.interval
    last_ts = args.since # this is so if the listener is activated we can call it back in time and then only call from latest.
    print(f'\n Starting listener mode (interval={interval}s)\n')

    while True:
        if last_ts is None:
            since = db.get_latest_timestamp()
        else: 
            since = last_ts
        print(f"\n Polling API (since={since})...")
        runner.run(since=since)
        last_ts = db.get_latest_timestamp()
        print(f"Sleeping for {interval} seconds... \n")
        time.sleep(interval) 


def cmd_preview(args):
    db = DBUtils(args.db)
    df = db.preview(n=args.n)
    print(df)


def cmd_drop(args):
    db = DBUtils(args.db)
    db.drop_table()


# ─────────────────────────────────────────────
# Parser Builder
# ─────────────────────────────────────────────

def build_parser():
    parser = argparse.ArgumentParser(
        description="NYC 311 ETL Pipeline Command Line Interface"
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Run Command
    p_run = subparsers.add_parser(
        "run",
        help="Run a single ETL ingestion cycle"
    )
    add_db_args(p_run)
    add_run_args(p_run)
    p_run.set_defaults(func=cmd_run)

    p_listen = subparsers.add_parser(
        "listen",
        help="Continuously poll the API every N seconds"
    )
    add_db_args(p_listen)
    add_run_args(p_listen)
    p_listen.add_argument(
        "--interval", 
        type=int,
        default=60,
        help="Polling interval in seconds (default 60)."
    )
    p_listen.set_defaults(func=cmd_listen)

    # Preview Command
    p_preview = subparsers.add_parser(
        "preview",
        help="Preview rows from the SQLite database"
    )
    add_db_args(p_preview)
    p_preview.add_argument(
        "--n",
        type=int,
        default=10,
        help="Number of rows to preview"
    )
    p_preview.set_defaults(func=cmd_preview)

    # Drop Command
    p_drop = subparsers.add_parser(
        "drop",
        help="Drop the requests table from the database"
    )
    add_db_args(p_drop)
    p_drop.set_defaults(func=cmd_drop)

    return parser


# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────

def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    import sys

    # IDE MODE: If no arguments provided, simulate a CLI call for convenience.
    if len(sys.argv) == 1:
        sys.argv.extend([
            "listen",
           # "--limit", "50",
            "--since", "2025-10-01"
        ])
        print(f"(Simulated CLI call) → python main.py {' '.join(sys.argv[1:])}")

    main()
