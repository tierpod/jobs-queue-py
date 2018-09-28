command-runner
==============

Listen for incoming requests on unix datagram socket. Executes command only if executable configured
in **commands** list. All arguments passes to executable.

Skips command if alredy cached.

Adds command to the queue (limit size: **queue_size**) if all workers are busy.

```
request ---> in cache? -> queue is full? -> workers
(cmd line)       |              |
                 |              ---yes----> skip
                 |
                 ---yes-------------------> skip
```

Usage and examples
------------------

```bash
# /bin/sleep must be configured in [commands] section
printf "/bin/sleep 10" | nc -uU /tmp/cmd-runner.socket
```
