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
from asyncio import StreamReader, StreamWriter, Queue, QueueFull
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

    def __contains__(self, item) -> bool:
        return item in self._cache

    def append(self, item: str) -> None:
        """Append `item` to cache"""

        if len(self._cache) >= self.maxsize:
            log.error("cache limit is exceeded")
            return

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

    def __init__(self, name: str, cache: Cache, allowed_commands: List[str], queue: Queue):
        self.name = name
        self.cache = cache
        self.allowed_commands = allowed_commands
        self.queue = queue
        self.shutdown = asyncio.Event()
        self._loop = asyncio.get_event_loop()

    async def run(self) -> None:
        while not self.shutdown.is_set():
            cmd = await self.queue.get()
            try:
                await self.exec(cmd)
            finally:
                self.queue.task_done()

        else:
            log.debug("(%s) worker shutdown", self.name)

    async def exec(self, cmd: str) -> None:
        log.info("(%s) exec  : %s", self.name, cmd)

        name, *opts = shlex.split(cmd)
        if name not in self.allowed_commands:
            log.warning("(%s) skip command '%s': not in the allowed commands list", self.name, name)
            return

        self.cache.append(cmd)
        log.info("(%s) exec  : %s", self.name, cmd)
        await self._wait_and_log(cmd, name, opts)
        self.cache.remove(cmd)

    async def _wait_and_log(self, cmd: str, name: str, opts: List[str]) -> None:
        try:
            p = await asyncio.create_subprocess_exec(
                name, *opts, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        except Exception as err:
            log.error(err)
            return

        self._loop.create_task(log_output(p.stdout, f"({self.name}) stdout"))
        self._loop.create_task(log_output(p.stderr, f"({self.name}) stderr"))

        try:
            await p.wait()
        except asyncio.CancelledError:
            p.terminate()
            log.info("(%s) subprocess pid=%d was terminated", self.name, p.pid)
            return
        if p.returncode != 0:
            log.error("(%s) exec  : %s failed with exit code %d", self.name, cmd, p.returncode)


class RequestHandler(object):

    def __init__(self, queue: Queue, cache: Cache):
        self.queue = queue
        self.cache = cache

    async def handle(self, reader: StreamReader, writer: StreamWriter) -> None:
        raw_data = await reader.readline()
        cmd_line = raw_data.decode().strip()
        log.debug("got request: %s", cmd_line)

        if not cmd_line:
            log.error("unable to parse parse data: '%s'", cmd_line)
            await self.reply(writer, Answer.SKIP)
            return

        if cmd_line in self.cache:
            log.error("skip: already in commands cache")
            await self.reply(writer, Answer.SKIP)
            return

        try:
            self.queue.put_nowait(cmd_line)
        except QueueFull:
            log.error("skip: queue limit is exceeded")
            await self.reply(writer, Answer.SKIP)
            return

        await self.reply(writer, Answer.OK)

    async def reply(self, writer: StreamWriter, answer: Answer) -> None:
        log.debug("send reply: %s", answer)
        writer.write(answer.value)
        await writer.drain()
        log.debug("close client connection")
        writer.close()


class Config(object):
    def __init__(self, socket: str, log_debug: bool, log_datetime: bool, cache_maxsize: int,
                 cache_delete_mode: CacheDeleteMode, cache_ttl: int, workers: int,
                 commands: List[str]):
        self.socket = socket
        self.log_debug = log_debug
        self.log_datetime = log_datetime
        self.cache_maxsize = cache_maxsize
        self.cache_delete_mode = cache_delete_mode
        self.cache_ttl = cache_ttl
        self.workers = workers
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
            workers=c.getint("main", "workers"),
            commands=commands,
        )


def shutdown(workers, signum=None):
    loop = asyncio.get_event_loop()
    log.info("shutting down server (%s)", signum)
    loop.stop()
    # shutdown all workers
    for w in workers:
        w.shutdown.set()
    # shutdown all pending coroutines
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
    queue = Queue(maxsize=cfg.workers)
    request_handler = RequestHandler(queue=queue, cache=cache)

    # start asynchronous workers
    workers = []
    for i in range(cfg.workers):
        w = Worker(name=f"Worker-{i+1}", allowed_commands=cfg.commands, cache=cache, queue=queue)
        loop.create_task(w.run())
        workers.append(w)

    # start asynchronous server
    handler_coro = asyncio.start_unix_server(request_handler.handle, path=cfg.socket)
    server = loop.run_until_complete(handler_coro)

    loop.add_signal_handler(signal.SIGTERM, partial(shutdown, workers, "TERM"))

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
