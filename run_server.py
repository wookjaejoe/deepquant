from server import WebSocketServer
import asyncio
from base import log

log.init()


def custom_exception_handler(loop, context):
    loop.default_exception_handler(context)
    loop.stop()


def main():
    loop = asyncio.new_event_loop()
    WebSocketServer(loop).init()
    loop.set_exception_handler(custom_exception_handler)
    loop.run_forever()


if __name__ == '__main__':
    main()
