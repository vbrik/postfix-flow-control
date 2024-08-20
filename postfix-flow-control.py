#!/usr/bin/env python3
import argparse
import sys
import logging
import pickle
from time import time, sleep
from bisect import bisect
import subprocess
import shlex
import socket
from logging.handlers import SysLogHandler
from contextlib import suppress
import json


DISABLED_MESSAGE = "postfix-flow-control-engaged"
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('postfix-flow-control')
logger.addHandler(SysLogHandler(address='/dev/log'))


def main():
    parser = argparse.ArgumentParser(
            description="",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--time-horizon", type=lambda x: int(x)*60*60, metavar="HOURS",
                        help="count events that occurred up to this many HOURS ago")
    parser.add_argument("--count-limit", type=int, default=4000, metavar='NUMBER',
                        help="maximum permissible count of events before the horizon")
    parser.add_argument("--backoff", type=int, metavar='SECONDS', default=600,
                        help="amount of time by which to defer deliveries")
    parser.add_argument("--history", metavar="PATH", default="/var/tmp/postfix-flow-control.pkl",
                        help="history file")
    parser.add_argument("--puppet-lockfile", metavar="PATH",
                        default="/opt/puppetlabs/puppet/cache/state/agent_disabled.lock",
                        help="path to puppet agent disabled lockfile")
    args = parser.parse_args()

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
        sys.exit(1)

    now = time()
    try:
        with open(args.state, "rb") as f:
            history = pickle.load(f)
    except FileNotFoundError:
        history = []

    history.append(now)
    cutoff = bisect(history, now - args.time_horizon)
    history = history[cutoff:]
    with open(args.state, 'wb') as f:
        pickle.dump(history, f)

    if len(history) > args.count_limit:
        with suppress(FileNotFoundError):
            agent_disabled = json.load(open(args.puppet_lockfile))
            puppet_disabled = agent_disabled.get("disabled_message")

        if puppet_disabled == DISABLED_MESSAGE:
            return 0

        subprocess.run(shlex.split(f"sudo puppet agent --disable {DISABLED_MESSAGE}")
        subprocess.run(shlex.split("sudo postconf -e defer_transports=smtp"))
        subprocess.run(shlex.split("sudo postfix reload"))
        subprocess.Popen(['bash', '-c', f'sleep {args.backoff};'
                                        f' sudo postconf -e defer_transports=;'
                                        f' sudo postfix reload;'
                                        f' sudo postfix flush;'
                                        f' sudo puppet agent --enable'],
                         start_new_session=True)



if __name__ == "__main__":
    sys.exit(main())

