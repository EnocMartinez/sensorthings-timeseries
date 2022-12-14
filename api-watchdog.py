#!/usr/bin/env python3
"""
Makes sure that the API is running. If not, reset it

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 6/9/22
"""

import os
import requests
from argparse import ArgumentParser
import logging
from common import setup_log
import psutil
import time


def is_process_running(arg_list):
    for proc in psutil.process_iter():
        try:
            found = True
            for arg in arg_list:
                if arg not in proc.cmdline():
                    found = False
                    break
            if found:
                return proc.pid
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return False


if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("-v", "--verbose", action="store_true", help="Shows verbose output", default=False)
    argparser.add_argument("-u", "--url", help="url that will be used", type=str,
                           default="http://localhost:5000/Sensors?$select=name&$top=1")
    argparser.add_argument("-l", "--log", help="Log directory", type=str, default="log")
    argparser.add_argument("-d", "--working-dir", help="The directory to be used", type=str,
                           default="/opt/sensorthings-api-expanded/")
    argparser.add_argument("-c", "--command", help="Command to be used in case of failure", type=str, required=True)

    args = argparser.parse_args()

    os.chdir(args.working_dir)  # change working dir
    os.makedirs(args.log, exist_ok=True)  # create log folder

    log = setup_log("watchdog", path=args.log, logger_name="watchdog", log_level="info")

    if args.verbose:
        log.setLevel(logging.DEBUG)

    pid = is_process_running(["python3", "api.py", "https://data.obsea.es/data-api"])

    if not pid:
        log.warning("Process is not running...")
        log.info(f"Trying to restart with command {args.command}")
        os.system(args.command)
        exit()

    log.info(f"API running with PID {pid}")

    failed = False
    try:
        r = requests.get(args.url)
        http_status = r.status_code
    except ConnectionError:
        failed = True
        http_status = -1

    if r.status_code > 299:
        failed = True

    if failed:
        log.warning(f"API is not running properly (HTTP status {http_status}), killing it")
        os.system(f"kill {pid}")
        time.sleep(5)
        log.info(f"Trying to restart with command {args.command}")
        os.system(args.command)

    else:
        log.info(f"API alive (http code {r.status_code})")







