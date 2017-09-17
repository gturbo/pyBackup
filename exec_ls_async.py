import asyncio
from asyncio.subprocess import PIPE, STDOUT

@asyncio.coroutine
def get_lines(shell_command):
    a,b,p = yield from asyncio.create_subprocess_shell(shell_command,
            stdin=PIPE, stdout=PIPE, stderr=STDOUT)
    return (yield from  p.communicate())[0].splitlines()
@asyncio.coroutine
def main():
    for l in get_lines("ls /tmp"):
        print(l)
    yield from p.wait()




loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.close()
