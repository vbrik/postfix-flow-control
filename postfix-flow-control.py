#!/usr/bin/env python3
import argparse
import logging
import shlex
import socket
import subprocess
import sys

from logging.handlers import SysLogHandler
from datetime import datetime, timedelta
from subprocess import check_output

# This is used in logwatch rules, so keep them in sync
APP_NAME = "POSTFIX-FLOW-CONTROL"
logger = logging.getLogger(APP_NAME)
logger.addHandler(SysLogHandler(address='/dev/log'))


# Can't control syslog format, so include details in message
# This is used in logwatch rules, so keep them in sync
def critical(msg):
    logger.critical(f"{APP_NAME} CRITICAL {msg}")


# Can't control syslog format, so include details in message
# This is used in logwatch rules, so keep them in sync
def warning(msg):
    logger.warning(f"{APP_NAME} WARNING {msg}")


def get_timestamp(log_line: str, year=datetime.now().year):
    parts = log_line.split(maxsplit=3)
    ts = ' '.join(parts[:3])
    dt = datetime.strptime(f"{year} {ts}", "%Y %b %d %H:%M:%S")
    return dt


def main():
    parser = argparse.ArgumentParser(
        description="A script to pause mail delivery if too many messages have been "
                    " relayed over a (long) period of time."
                    " The original motivation was to have a mechanism to keep Gmail from"
                    " imposing bulk sender restrictions on us. At the time of writing"
                    " the threshold was 5000 messages over 24 hours (this is playing it"
                    " very safe; the real rules are more complicated)."
                    " To work properly, mail log needs to rotated at least on Jan 1st."
                    " THIS IS A HACK THAT MESSES WITH PUPPET AND POSTFIX.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--time-window", type=int, default=86400, metavar="SECONDS",
                        help="count relays that occurred up to this many SECONDS ago")
    parser.add_argument("--relay-count-limit", type=int, default=4000, metavar='NUMBER',
                        help="maximum permissible count of relays within the time window")
    parser.add_argument("--mail-log", metavar="PATH", default="/var/log/maillog",
                        help="postfix log file")
    args = parser.parse_args()

    lock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        lock.bind(f"\0{APP_NAME}")
    except socket.error:
        critical("Failed to obtain lock")
        return 1

    with open(args.mail_log) as file:
        relay_logs = [line for line in file
                      if all(part in line for part in
                             ("relay=smtp-relay.gmail.com", "status=sent (250 2.0.0 OK"))]
    now = datetime.now()
    time_window = timedelta(seconds=args.time_window)
    recent_relays = [line for line in relay_logs
                     if now - get_timestamp(line) < time_window]

    postfix_is_deferring = b"smtp" in check_output("postconf -h defer_transports".split())
    try:
        agent_disabled = open("/opt/puppetlabs/puppet/cache/state/agent_disabled.lock").read()
        puppet_is_disabled = APP_NAME in agent_disabled
    except FileNotFoundError:
        puppet_is_disabled = False

    if postfix_is_deferring:
        if len(recent_relays) < 0.9 * args.relay_count_limit:
            warning(f"RESUMING MAIL TRANSPORTS")
            subprocess.run("postconf -e defer_transports=".split())
            subprocess.run("postfix reload".split())
            subprocess.run("postfix flush".split())
            if puppet_is_disabled:
                subprocess.run("puppet agent --enable".split())
    # if postfix is not deferring
    elif len(recent_relays) > args.relay_count_limit:
        if len(recent_relays) > 2 * args.relay_count_limit:
            warning(f"{len(recent_relays)} relays found. Has the year rolled over?")

        warning(f"DEFERRING MAIL TRANSPORTS. Processed {len(recent_relays)} >"
                f" {args.relay_count_limit} in the last {args.time_window / 60 / 60} hours.")

        subprocess.run(shlex.split(f"sudo puppet agent --disable '{APP_NAME}'"))
        subprocess.run("postconf -e defer_transports=smtp".split())
        subprocess.run("postfix reload".split())


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        critical(f"EXCEPTION {e}")
        raise
