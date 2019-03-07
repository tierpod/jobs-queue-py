#!/usr/bin/env python
# coding: utf8

"""Execute commands with limited queue.
"""

import argparse
import asyncio
import configparser
import logging
import os
import shlex
import signal
from asyncio import StreamReader, StreamWriter
from collections import namedtuple
from enum import Enum
from functools import partial
from typing import List

log = logging.getLogger()

DEFAULT_CFG_PATH = "/etc/cmd-runner.ini"


Command = namedtuple("Command", ["cmdline", "executable", "args"])


class Answer(Enum):
    OK = b"OK\n"
    SKIP = b"SKIP\n"


class CacheDeleteMode(Enum):
    EXPIRE = "expire"
    COMPLETE = "complete"
    EXPIRE_COMPLETE = "expire_complete"


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
    def __init__(self, mode: CacheDeleteMode, ttl: int):
        self.mode = mode
        self.ttl = ttl
        self._cache: List[str] = []
        self._loop = asyncio.get_event_loop()

    def __str__(self):
        return "<Cache(mode={mode} size={size})>".format(
            mode=self.mode.value, size=len(self._cache))

    def _remove(self, item):
        if item in self._cache:
            self._cache.remove(item)
        log.debug("%s remove %s", self, item)

    def remove(self, item):
        # can we remove item from cache after ttl time?
        if self.mode is CacheDeleteMode.COMPLETE or self.mode is CacheDeleteMode.EXPIRE_COMPLETE:
            self._remove(item)

    def append(self, item):
        log.debug("%s append %s", self, item)
        self._cache.append(item)
        # remove item from cache later (after ttl seconds), call another coroutine
        if self.mode is CacheDeleteMode.EXPIRE or self.mode is CacheDeleteMode.EXPIRE_COMPLETE:
            self._loop.call_later(self.ttl, lambda: self._remove(item))

    def __contains__(self, item):
        return item in self._cache


class RequestHandler:

    def __init__(self, queue: asyncio.Queue, cache: Cache):
        self.queue = queue
        self.cache = cache

    async def handle(self, reader: StreamReader, writer: StreamWriter) -> None:
        raw_data = await reader.readline()
        cmd_line = raw_data.decode().strip()
        log.debug("got request: %s", cmd_line)

        try:
            executable, *args = shlex.split(cmd_line)
        except ValueError as err:
            log.error("parse data: %s", err)
            await self.reply(writer, Answer.SKIP)
            return

        cmd = Command(cmdline=cmd_line, executable=executable, args=args)
        if cmd.cmdline in self.cache:
            log.info("skip command: already in cache")
            await self.reply(writer, Answer.SKIP)
            return

        try:
            self.queue.put_nowait(cmd)
        except asyncio.QueueFull:
            log.error("queue limit is exceeded")
            await self.reply(writer, Answer.SKIP)
            return

        await self.reply(writer, Answer.OK)

    async def reply(self, writer: StreamWriter, answer: Answer) -> None:
        log.debug("send reply: %s", answer)
        writer.write(answer.value)
        await writer.drain()
        log.debug("close client connection")
        writer.close()


class Worker:

    def __init__(self, name: str, queue: asyncio.Queue, cache: Cache, allowed_commands: List[str]):
        self.name = name
        self.queue = queue
        self.cache = cache
        self.allowed_commands = allowed_commands
        self.shutdown = asyncio.Event()
        self._loop = asyncio.get_event_loop()

    async def run(self) -> None:
        log.info("(%s) start worker", self.name)
        while not self.shutdown.is_set():
            cmd = await self.queue.get()
            if cmd.executable not in self.allowed_commands:
                log.warning("(%s) skip '%s': not in the commands list", self.name, cmd.executable)
                continue

            self.cache.append(cmd.cmdline)
            await self.exec(cmd)
            self.cache.remove(cmd.cmdline)
            self.queue.task_done()
        log.debug("(%s) worker stopped", self.name)

    def stop(self) -> None:
        self.shutdown.set()

    async def exec(self, cmd: Command) -> None:
        log.info("(%s) exec  : %s", self.name, cmd.cmdline)
        try:
            p = await asyncio.create_subprocess_exec(
                cmd.executable, *cmd.args,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
        except Exception as err:  # pylint: disable=broad-except
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
            log.error("(%s) exec  : %s failed with exit code %d", self.name, cmd.cmdline,
                      p.returncode)


class Config(object):
    def __init__(self, socket: str, workers: int, log_debug: bool, log_datetime: bool,
                 queue_size: int, cache_delete_mode: CacheDeleteMode, cache_expire: int,
                 commands: List[str]):
        self.socket = socket
        self.workers = workers
        self.log_debug = log_debug
        self.log_datetime = log_datetime
        self.queue_size = queue_size
        self.cache_delete_mode = cache_delete_mode
        self.cache_expire = cache_expire
        self.commands = commands

    @classmethod
    def from_file(cls, path: str) -> "Config":
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
            workers=c.getint("main", "workers"),
            log_debug=c.getboolean("main", "log_debug"),
            log_datetime=c.getboolean("main", "log_datetime"),
            queue_size=c.getint("main", "queue_size"),
            cache_delete_mode=cache_delete_mode,
            cache_expire=c.getint("main", "cache_expire"),
            commands=commands,
        )


def shutdown(signame: str, workers: List[Worker]) -> None:
    log.info("server shutdown (%s)", signame)
    loop = asyncio.get_event_loop()
    loop.stop()
    # stop all running workers
    [w.stop() for w in workers]
    # stop all pending coroutines
    pending = asyncio.Task.all_tasks()
    [t.cancel() for t in pending]
    asyncio.ensure_future(asyncio.gather(*pending, return_exceptions=True))


def main():
    args = parse_args()
    config = Config.from_file(args.config)

    global log  # pylint: disable=W0603
    log = configure_logger(config.log_debug, config.log_datetime)

    log.info("load configuration from %s, %d commands loaded", args.config, len(config.commands))

    loop = asyncio.get_event_loop()
    cache = Cache(mode=config.cache_delete_mode, ttl=config.cache_expire)
    queue = asyncio.Queue(maxsize=config.queue_size)
    request_handler = RequestHandler(queue=queue, cache=cache)

    # start asynchronous workers
    workers = []
    for i in range(config.workers):
        w = Worker(name=f"wrk{i+1}", allowed_commands=config.commands, cache=cache, queue=queue)
        loop.create_task(w.run())
        workers.append(w)

    # start asynchronous server
    handler_coro = asyncio.start_unix_server(request_handler.handle, path=config.socket)
    server = loop.run_until_complete(handler_coro)

    loop.add_signal_handler(signal.SIGTERM, partial(shutdown, "TERM", workers))

    log.info("listen unix socket on %s", config.socket)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        shutdown("KeyboardInterrupt", workers)
    finally:
        server.close()
        loop.run_until_complete(server.wait_closed())
        loop.close()
        os.unlink(config.socket)


if __name__ == "__main__":
    main()
