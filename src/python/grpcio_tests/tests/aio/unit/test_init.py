import asyncio
import logging
import unittest

from unittest.mock import patch

from grpc.experimental import aio


class TestInsecureChannel(unittest.TestCase):
    def test_insecure_channel(self):
        async def coro():
            channel = aio.insecure_channel('target:port')
            self.assertIsInstance(channel, aio.Channel)

        with patch("grpc._cython.cygrpc"):
            asyncio.get_event_loop().run_until_complete(coro())


if __name__ == '__main__':
    logging.basicConfig()
    unittest.main(verbosity=2)
