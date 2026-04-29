#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Simple log cleanup / rotation script for the Alert Management scheduler.

Usage (examples):
  # Dry-run: show what would be deleted/rotated
  python3 log_cleanup.py --dry-run

  # Delete logs older than 7 days (default), rotate main logs over 100MB
  python3 log_cleanup.py

  # Custom settings
  python3 log_cleanup.py --log-dir /data/odoo2/custom_addons/icomply_odoo/alert_management/logs \
                         --max-age-days 14 \
                         --max-size-mb 200

This script is standalone and does not depend on Odoo. It can be:
- Run manually on this server
- Scheduled via cron on the host
- Used inside a Docker/Compose sidecar container with the logs directory mounted
"""

import argparse
import logging
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path


DEFAULT_MAX_AGE_DAYS = 1
DEFAULT_MAX_SIZE_MB = 20

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_LOG_DIR = SCRIPT_DIR.parent / "logs"

MAIN_LOG_FILES = {"alert_scheduler.log"}


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def rotate_main_log_file(log_file: Path, max_size_bytes: int, dry_run: bool = False) -> None:
    """
    If a main log file exceeds max_size_bytes, rotate it:
    - Copy current file to <name>_YYYYMMDD_HHMMSS.log
    - Truncate the original file
    """
    try:
        if not log_file.is_file():
            return

        size = log_file.stat().st_size
        if size <= max_size_bytes:
            logging.debug(f"Main log within size limit: {log_file} ({size} bytes)")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        rotated_name = f"{log_file.stem}_{timestamp}{log_file.suffix}"
        rotated_path = log_file.with_name(rotated_name)

        logging.info(
            "Rotating main log %s (size=%d bytes) -> %s",
            log_file.name,
            size,
            rotated_path.name,
        )

        if not dry_run:
            # Copy current log to rotated file
            shutil.copy2(log_file, rotated_path)
            # Truncate original file
            with log_file.open("w"):
                pass

    except Exception as e:
        logging.warning("Failed to rotate log %s: %s", log_file, e)


def delete_old_logs(log_dir: Path, max_age_days: int, dry_run: bool = False) -> None:
    """
    Delete log files in log_dir (except main log files) older than max_age_days.
    """
    if not log_dir.exists():
        logging.info("Log directory does not exist: %s", log_dir)
        return

    cutoff = datetime.now() - timedelta(days=max_age_days)
    logging.info("Deleting log files in %s older than %s", log_dir, cutoff.isoformat())

    deleted_count = 0

    for path in log_dir.iterdir():
        if not path.is_file():
            continue

        if path.name in MAIN_LOG_FILES:
            # Leave main logs to rotation logic
            continue

        try:
            mtime = datetime.fromtimestamp(path.stat().st_mtime)
        except OSError as e:
            logging.warning("Could not read mtime for %s: %s", path, e)
            continue

        if mtime < cutoff:
            logging.info("Deleting old log: %s (mtime=%s)", path.name, mtime.isoformat())
            if not dry_run:
                try:
                    path.unlink()
                    deleted_count += 1
                except Exception as e:
                    logging.warning("Failed to delete %s: %s", path, e)

    logging.info("Deleted %d old log file(s)", deleted_count)


def main() -> None:
    parser = argparse.ArgumentParser(description="Cleanup/rotate Alert Management logs")
    parser.add_argument(
        "--log-dir",
        type=str,
        default=str(DEFAULT_LOG_DIR),
        help=f"Log directory (default: {DEFAULT_LOG_DIR})",
    )
    parser.add_argument(
        "--max-age-days",
        type=int,
        default=DEFAULT_MAX_AGE_DAYS,
        help=f"Delete non-main log files older than this many days (default: {DEFAULT_MAX_AGE_DAYS})",
    )
    parser.add_argument(
        "--max-size-mb",
        type=int,
        default=DEFAULT_MAX_SIZE_MB,
        help=f"Rotate main logs when they exceed this size in MB (default: {DEFAULT_MAX_SIZE_MB})",
    )
    parser.add_argument(
        "--no-rotate-main",
        action="store_true",
        help="Disable rotation of main log files (only delete old logs)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without deleting or modifying any files",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    log_dir = Path(args.log_dir)
    max_size_bytes = args.max_size_mb * 1024 * 1024

    logging.info("Log cleanup starting")
    logging.info("Log directory: %s", log_dir)
    logging.info("Max age (days): %d", args.max_age_days)
    logging.info("Max main log size: %d MB (rotation %s)",
                 args.max_size_mb,
                 "DISABLED" if args.no_rotate_main else "ENABLED")
    logging.info("Dry-run: %s", args.dry_run)

    if not args.no_rotate_main:
        for main_name in MAIN_LOG_FILES:
            main_path = log_dir / main_name
            rotate_main_log_file(main_path, max_size_bytes, dry_run=args.dry_run)

    delete_old_logs(log_dir, args.max_age_days, dry_run=args.dry_run)

    logging.info("Log cleanup finished")


if __name__ == "__main__":
    main()

