#!/usr/bin/env python
# coding: utf8
# pylint: disable=C0111

"""Execute commands. Limit amount of running commands.
"""

import argparse
import asyncio
import configparser
import logging
import os
import shlex
import signal
from asyncio import StreamReader, StreamWriter
from enum import Enum
from functools import partial
from typing import List


log = logging.getLogger()

DEFAULT_CFG_PATH = "/etc/cmd-runner.ini"


class CacheDeleteMode(Enum):
    EXPIRE = "expire"
    COMPLETE = "complete"
    EXPIRE_COMPLETE = "expire_complete"


class Answer(Enum):
    OK = b"OK\n"
    SKIP = b"SKIP\n"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("-c", "--config", type=str, default=DEFAULT_CFG_PATH, help="path to config file")
    return p.parse_args()


def configure_logger(debug: bool, datetime: bool) -> logging.Logger:
    lvl = logging.DEBUG if debug else logging.INFO
    fmt = "%(levelname)-7s %(message)s"

    if datetime:
        fmt = "%(asctime)s " + fmt

    logging.basicConfig(format=fmt, datefmt="%Y-%m-%d/%H:%M:%S", level=lvl)
    return logging.getLogger()


async def log_output(fh: StreamReader, prefix: str) -> None:
    """Log output from `fh` with prefix `prefix`"""

    while True:
        try:
            line = await fh.readline()
        except asyncio.CancelledError:
            break
        if not line:
            break
        log.info("%s: %s", prefix, line.decode().strip())


class Cache(object):
    """Limited cache"""

    def __init__(self, mode: CacheDeleteMode, maxsize: int, ttl: int):
        self.mode = mode
        self.maxsize = maxsize
        self.ttl = ttl
        self.loop = asyncio.get_event_loop()
        self._cache: List[str] = []

    def __repr__(self) -> str:
        return "Cache(mode={mode}, size={size}/{maxsize})".format(
            mode=self.mode, size=len(self._cache), maxsize=self.maxsize)

    def append(self, item: str) -> None:
        """Append `item` to cache.

        Raises:
            ValueError: if unable to append item to cache.
        """

        if item in self._cache:
            raise ValueError("already in cache")

        if len(self._cache) >= self.maxsize:
            raise ValueError("cache limit is exceeded")

        log.debug("%s append %s", self, item)
        self._cache.append(item)

        # we want to remove item from cache later (after ttl seconds), call another coroutine
        if self.mode is CacheDeleteMode.EXPIRE or self.mode is CacheDeleteMode.EXPIRE_COMPLETE:
            self.loop.call_later(self.ttl, lambda: self._remove(item))

    def _remove(self, item: str) -> None:
        if item in self._cache:
            self._cache.remove(item)
            log.debug("%s remove %s", self, item)

    def remove(self, item: str) -> None:
        """Remove `item` from cache"""

        if self.mode is CacheDeleteMode.COMPLETE or self.mode is CacheDeleteMode.EXPIRE_COMPLETE:
            self._remove(item)


class Worker(object):

    def __init__(self, cache: Cache, allowed_commands: List[str], running_limit: int):
        self.cache = cache
        self.allowed_commands = allowed_commands
        self.running_limit = running_limit
        self._loop = asyncio.get_event_loop()
        self._running = 0

    async def exec(self, cmd: str) -> Answer:
        """Validate `cmd` and execute it"""

        log.info("exec  : %s", cmd)

        name, *args = shlex.split(cmd)
        if name not in self.allowed_commands:
            log.warning("skip command '%s': not in the allowed commands list", name)
            return Answer.SKIP

        if self._running >= self.running_limit:
            log.error("maximum number of running workers exceeded: %d/%d", self._running,
                      self.running_limit)
            return Answer.SKIP

        try:
            self.cache.append(cmd)
        except ValueError as err:
            log.error(err)
            return Answer.SKIP

        # don't wait for complete process inside this coroutine, create another one
        self._loop.create_task(self._wait_and_log(cmd, name, args))

        return Answer.OK

    async def _wait_and_log(self, cmd, name, args) -> None:
        try:
            p = await asyncio.create_subprocess_exec(
                name, *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        except Exception as err:
            log.error(err)
            return

        self._running += 1

        self._loop.create_task(log_output(p.stdout, "stdout"))
        self._loop.create_task(log_output(p.stderr, "stderr"))

        try:
            await p.wait()
        except asyncio.CancelledError:
            p.terminate()
            log.info("subprocess pid=%d is terminated", p.pid)
            return
        if p.returncode != 0:
            log.error("exec  : %s failed with exit code %d", cmd, p.returncode)
        self.cache.remove(cmd)
        self._running -= 1


class RequestHandler(object):

    def __init__(self, worker: Worker):
        self.worker = worker

    async def handle(self, reader: StreamReader, writer: StreamWriter) -> None:
        raw_data = await reader.readline()
        cmd_line = raw_data.decode().strip()
        log.debug("got request: %s", cmd_line)

        answer = await self.worker.exec(cmd_line)

        log.debug("send reply: %s", answer)
        writer.write(answer.value)
        await writer.drain()

        log.debug("close client connection")
        writer.close()


class Config(object):
    def __init__(self, socket: str, log_debug: bool, log_datetime: bool, cache_maxsize: int,
                 cache_delete_mode: CacheDeleteMode, cache_ttl: int, running_limit: int,
                 commands: List[str]):
        self.socket = socket
        self.log_debug = log_debug
        self.log_datetime = log_datetime
        self.cache_maxsize = cache_maxsize
        self.cache_delete_mode = cache_delete_mode
        self.cache_ttl = cache_ttl
        self.running_limit = running_limit
        self.commands = commands

    @classmethod
    def from_ini(cls, path: str) -> "Config":
        """Read ini file and create Config from it"""

        c = configparser.ConfigParser(allow_no_value=True)
        c.read(path)
        cache_delete_mode = CacheDeleteMode(c.get("main", "cache_delete_mode"))

        commands = []
        for k, _ in c.items("commands"):
            commands.append(k)

        if not commands:
            raise ValueError("no commands defined in config")

        return cls(
            socket=c.get("main", "socket"),
            log_debug=c.getboolean("main", "log_debug"),
            log_datetime=c.getboolean("main", "log_datetime"),
            cache_maxsize=c.getint("main", "cache_maxsize"),
            cache_delete_mode=cache_delete_mode,
            cache_ttl=c.getint("main", "cache_ttl"),
            running_limit=c.getint("main", "running_limit"),
            commands=commands,
        )


def shutdown(signum=None):
    loop = asyncio.get_event_loop()
    log.info("shutting down server (%s)", signum)
    loop.stop()
    pending = asyncio.Task.all_tasks()
    [t.cancel() for t in pending]
    asyncio.ensure_future(asyncio.gather(*pending, return_exceptions=True))


def main():
    args = parse_args()
    cfg = Config.from_ini(args.config)

    global log
    log = configure_logger(cfg.log_debug, cfg.log_datetime)

    log.info("load configuration from %s, %d commands loaded", args.config, len(cfg.commands))

    loop = asyncio.get_event_loop()
    cache = Cache(mode=cfg.cache_delete_mode, maxsize=cfg.cache_maxsize, ttl=cfg.cache_ttl)
    worker = Worker(allowed_commands=cfg.commands, cache=cache, running_limit=cfg.running_limit)
    request_handler = RequestHandler(worker=worker)
    handler_coro = asyncio.start_unix_server(request_handler.handle, path=cfg.socket)
    server = loop.run_until_complete(handler_coro)

    loop.add_signal_handler(signal.SIGTERM, partial(shutdown, "TERM"))

    log.info("listen unix socket on %s", cfg.socket)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        shutdown("KeyboardInterrupt")
    finally:
        # close the server
        # loop.run_until_complete(loop.shutdown_asyncgens())
        server.close()
        loop.run_until_complete(server.wait_closed())

    loop.close()
    os.unlink(cfg.socket)


if __name__ == "__main__":
    main()
