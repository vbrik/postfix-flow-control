#!/usr/bin/env python3
import argparse
import logging
import os
import os.path
import pickle
import shlex
import socket
import subprocess
import sys

from bisect import bisect
from contextlib import suppress
from logging.handlers import SysLogHandler
from time import time, sleep

APP_NAME = "postfix-flow-control"
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(APP_NAME)
logger.addHandler(SysLogHandler(address='/dev/log'))


def main():
    parser = argparse.ArgumentParser(
        description="XXX this doesn't really work."
                    "Simple script to pause mail delivery if too many messages were"
                    " delivered over a (long) period of time. The original motivation"
                    " was to have a mechanism to keep Gmail from imposing bulk sender"
                    " restrictions on us. For this to work, postfix needs to be configured"
                    " to always_bcc to a user, and that user's .procmailrc needs to call "
                    " this script. THIS IS A HACK THAT MESSES WITH PUPPET AND POSTFIX.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--time-horizon", type=int, default=86400, metavar="SECONDS",
                        help="count events that occurred up to this many SECONDS ago")
    parser.add_argument("--count-limit", type=int, default=3500, metavar='NUMBER',
                        help="maximum permissible count of events before the horizon")
    parser.add_argument("--backoff", type=int, metavar='SECONDS', default=600,
                        help="duration to pause deliveries")
    parser.add_argument("--history", metavar="FILENAME", default="postfix-flow-control-history.pkl",
                        help="history file, relative to procmail's $MAILDIR environment variable")
    args = parser.parse_args()

    history_path = os.path.join(os.environ['MAILDIR'], args.history)

    lock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    for attempt in range(10):
        try:
            lock.bind('\0postfix-flow-control')
        except socket.error:
            sleep(0.1)
            continue
        break
    else:
        logger.critical("Failed to obtain lock")
        return 1

    now = time()
    try:
        with open(history_path, "rb") as f:
            history = pickle.load(f)
    except FileNotFoundError:
        history = []

    history.append(now)
    cutoff = bisect(history, now - args.time_horizon)
    history = history[cutoff:]

    if len(history) > args.count_limit:
        logger.warning(f"DEFERRING MAIL TRANSPORTS. Processed {len(history)} >"
                       f" {args.count_limit} in the last {args.time_horizon / 60 / 60} hours.")
        with suppress(FileNotFoundError):

        subprocess.run(shlex.split(f"sudo puppet agent --disable '{APP_NAME}'"))
        subprocess.run(shlex.split("sudo postconf -e defer_transports=smtp"))
        subprocess.run(shlex.split("sudo postfix reload"))
        subprocess.Popen(['bash', '-c', f'sleep {args.backoff};'
                                        f' sudo postconf -e defer_transports=;'
                                        f' sudo postfix reload;'
                                        f' sudo postfix flush;'
                                        f' sudo puppet agent --enable'],
                         start_new_session=True)

    with open(history_path, 'wb') as f:
        pickle.dump(history, f)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        logger.critical(f"{APP_NAME} ENCOUNTERED AN EXCEPTION {e}")
        raise
