import asyncio

import aiohttp


@asyncio.coroutine
def hello(session):
    r = yield from session.get(url="http://www.google.com/", allow_redirects=False)
    body = yield from r.read()
    return body


@asyncio.coroutine
def main():
    with aiohttp.ClientSession() as session:
        resp = yield from hello(session)
        print(resp)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.close()
