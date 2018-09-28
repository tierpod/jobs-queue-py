#!/usr/bin/env python
# coding: utf8
# pylint: disable=C0111

"""Execute commands with limited queue.
"""

import argparse
import ConfigParser
import logging
import os
import Queue
import shlex
import signal
import subprocess
import threading
from SocketServer import UnixDatagramServer, DatagramRequestHandler

import cachetools


DEFAULT_CFG_PATH = "/etc/cmd-runner.ini"

CACHE_DELETE_EXPIRE = "expire"
CACHE_DELETE_COMPLETE = "complete"
CACHE_DELETE_EXPIRE_COMPLETE = "expire_complete"


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("-c", "--config", type=str, default=DEFAULT_CFG_PATH, help="path to config file")
    return p.parse_args()


def configure_logger(debug, datetime):
    lvl = logging.DEBUG if debug else logging.INFO

    if datetime:
        fmt = "%(asctime)s %(threadName)-10s %(levelname)-7s %(message)s"
    else:
        fmt = "%(threadName)-10s %(levelname)-7s %(message)s"

    logging.basicConfig(format=fmt, datefmt="%Y-%m-%d/%H:%M:%S", level=lvl)
    return logging.getLogger()


# pylint: disable=W0621,W0613
def signal_handler(*args):
    log.info("server shutdown")
    exit()


class RequestHandler(DatagramRequestHandler):
    def handle(self):
        data = self.request[0].strip()
        log.debug("got request: %s", data)

        try:
            cmd = Command.from_data(data)
        except ValueError as err:
            log.error("parse data: %s", err)
            return

        if cmd.key in cache:
            log.info("skip command: already in cache")
            return

        try:
            queue.put(cmd, block=False, timeout=1)
        except Queue.Full:
            log.error("queue limit is exceeded")
            return

    def finish(self):
        """Workaround for unix socket datagram server:

        self.socket.sendto(self.wfile.getvalue(), self.client_address)
            error: [Errno 2] No such file or directory
        """

        pass


class Worker(threading.Thread):
    def __init__(self, name, queue, cache):
        super(Worker, self).__init__(name=name)
        self.daemon = True

        log.info("start worker %s", self.name)
        self.queue = queue
        self.cache = cache

    def run(self):
        while True:
            cmd = self.queue.get()
            self.cache[cmd.key] = None
            cmd.execute()
            del self.cache[cmd.key]


class Cache(object):
    def __init__(self, mode, maxsize, ttl):
        self.mode = mode
        if mode == CACHE_DELETE_COMPLETE:
            self.cache = cachetools.Cache(maxsize=maxsize)
        else:
            self.cache = cachetools.TTLCache(maxsize=maxsize, ttl=ttl)

    def __str__(self):
        return str(self.cache)

    def __delitem__(self, key):
        if self.mode == CACHE_DELETE_EXPIRE:
            return

        try:
            self.cache.__delitem__(key)
        except KeyError:
            return

    def __setitem__(self, key, value):
        self.cache.__setitem__(key, value)

    def __getitem__(self, key):
        return self.cache.__getitem__(key)

    def __contains__(self, key):
        return self.cache.__contains__(key)


class Command(object):
    def __init__(self, args):
        self.args = args

    def __str__(self):
        return "{}".format(" ".join(self.args))

    @property
    def key(self):
        return str(self)

    def execute(self):
        log.info("exec  : %s", self.args)
        p = subprocess.Popen(self.args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdout, stderr) = p.communicate()

        for line in stdout.split("\n"):
            if line:
                log.info("stdout: %s", line)

        for line in stderr.split("\n"):
            if line:
                log.info("stderr: %s", line)

        if p.returncode != 0:
            log.error("exec  : %s failed with %d", self.args, p.returncode)

    @classmethod
    def from_data(cls, data):
        cmd = shlex.split(data)
        if not cmd:
            raise ValueError("unable to parse data")

        name = cmd[0]
        args = cmd[1:]
        executable = config.get_command(name)

        return cls(args=[executable] + args)


class Config(object):
    def __init__(self, socket, workers, log_debug, log_datetime, queue_size, cache_delete_mode,
                 cache_expire, commands):
        self.socket = socket
        self.workers = workers
        self.log_debug = log_debug
        self.log_datetime = log_datetime
        self.queue_size = queue_size
        self.cache_delete_mode = cache_delete_mode
        self.cache_expire = cache_expire
        self.commands = commands

    @classmethod
    def from_file(cls, path):
        c = ConfigParser.ConfigParser()
        c.read(path)
        cache_delete_mode = c.get("main", "cache_delete_mode")
        if cache_delete_mode not in (CACHE_DELETE_EXPIRE, CACHE_DELETE_COMPLETE,
                                     CACHE_DELETE_EXPIRE_COMPLETE):
            raise ValueError("wrong cache_delete_mode value")

        commands = dict(c.items("commands"))

        if not commands:
            raise ValueError("no commands defined in config")

        return cls(
            socket=c.get("main", "socket"),
            workers=c.getint("main", "workers"),
            log_debug=c.getboolean("main", "log_debug"),
            log_datetime=c.getboolean("main", "log_datetime"),
            queue_size=c.getint("main", "queue_size"),
            cache_delete_mode=cache_delete_mode,
            cache_expire=c.getint("main", "cache_expire"),
            commands=commands,
        )

    def get_command(self, name):
        if name not in self.commands:
            raise ValueError("command '%s' not configured" % name)

        return self.commands[name]


if __name__ == "__main__":
    args = parse_args()
    config = Config.from_file(args.config)

    log = configure_logger(config.log_debug, config.log_datetime)

    log.debug("load configuration from %s, %d commands loaded", args.config, len(config.commands))

    signal.signal(signal.SIGTERM, signal_handler)

    cache = Cache(mode=config.cache_delete_mode, maxsize=config.queue_size, ttl=config.cache_expire)

    queue = Queue.Queue(maxsize=config.queue_size)
    for i in range(1, config.workers + 1):
        worker = Worker(name="Worker-%d" % i, queue=queue, cache=cache)
        worker.start()

    log.info("listen unix socket on %s", config.socket)
    listen = UnixDatagramServer(config.socket, RequestHandler)
    try:
        listen.serve_forever()
    except KeyboardInterrupt:
        log.info("shutting down server")
        exit(0)
    finally:
        os.unlink(config.socket)
