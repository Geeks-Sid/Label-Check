"""Nightly materialization of per-user activity CSV files."""

import argparse
import datetime
import logging
import os
import time

os.environ["LABEL_CHECK_STATS_SCHEDULER"] = "true"

import app as app_module


def rollup() -> None:
    cutoff = datetime.date.today() - datetime.timedelta(days=1)
    app_module.user_manager.load()
    for user in app_module.user_manager.get_all():
        path = app_module.stats_store.rollup_user(str(user.id), cutoff)
        logging.info("Updated statistics through %s: %s", cutoff, path)


def run_forever() -> None:
    last_run_for_date = None
    while True:
        today = datetime.date.today()
        if last_run_for_date != today:
            rollup()
            last_run_for_date = today
        time.sleep(30)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    arguments = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if arguments.once:
        rollup()
    else:
        run_forever()


if __name__ == "__main__":
    main()
