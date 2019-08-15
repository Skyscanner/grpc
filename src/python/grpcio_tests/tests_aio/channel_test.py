# Copyright 2019 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import concurrent
import logging
import multiprocessing
import time
import unittest

from grpc.experimental import aio
from src.proto.grpc.testing import messages_pb2
from tests_aio import sync_server


class ChannelTest(unittest.TestCase):
    def setUp(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._server = sync_server.Server()
        self._server.start()
        aio.init_grpc_aio()

    def tearDown(self):
        self._server.terminate()

    def test_unary_unary_returns_multicallable(self):
        async def coro():
            channel = aio.insecure_channel('target:port')
            self.assertIsInstance(
                channel.unary_unary('/grpc.testing.TestService/UnaryCall'),
                aio.UnaryUnaryMultiCallable
            )

        asyncio.get_event_loop().run_until_complete(coro())

    def test_async_context(self):
        async def coro():
            async with aio.insecure_channel('localhost:%d' % sync_server.Server.PORT) as channel:
                hi = channel.unary_unary(
                    '/grpc.testing.TestService/UnaryCall',
                    request_serializer=messages_pb2.SimpleRequest.SerializeToString,
                    response_deserializer=messages_pb2.SimpleResponse.FromString
                )
                response = await hi(messages_pb2.SimpleRequest())

        asyncio.get_event_loop().run_until_complete(coro())


def _grpc_blocking_call():
    with grpc.insecure_channel('localhost:%d' % sync_server.Server.PORT) as channel:
        hi = channel.unary_unary(
            '/grpc.testing.TestService/UnaryCall',
            request_serializer=messages_pb2.SimpleRequest.SerializeToString,
            response_deserializer=messages_pb2.SimpleResponse.FromString
        )
        response = hi(messages_pb2.SimpleRequest())
        return True


def _grpc_aio_call():
    async def coro():
        aio.init_grpc_aio()
        async with aio.insecure_channel('localhost:%d' % sync_server.Server.PORT) as channel:
            hi = channel.unary_unary(
                '/grpc.testing.TestService/UnaryCall',
                request_serializer=messages_pb2.SimpleRequest.SerializeToString,
                response_deserializer=messages_pb2.SimpleResponse.FromString
            )
            response = await hi(messages_pb2.SimpleRequest())
            return True

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro())


async def _run_in_another_process(function):
    loop = asyncio.get_event_loop()
    with concurrent.futures.ProcessPoolExecutor() as pool:
        return await loop.run_in_executor(pool, function)


class InsecureChannelTest(unittest.TestCase):
    def setUp(self):
        self._server = sync_server.Server()
        self._server.start()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        aio.init_grpc_aio()

    def tearDown(self):
        self._server.terminate()

    def test_aio_supports_fork_and_grpc_blocking_usable(self):
        # We double check that once the Aio is initialized a fork syscall can
        # be executed and the child process can use the synchronous version of
        # the gRPC library.

        successful_call = asyncio.get_event_loop().run_until_complete(
            _run_in_another_process(_grpc_blocking_call)
        )
        self.assertEqual(successful_call, True)

    def test_aio_supports_fork_and_grpc_aio_usable(self):
        # We double check that once the Aio is initialized a fork syscall can
        # be executed and the child process can use the Aio version of the gRPC
        # library.

        successful_call = asyncio.get_event_loop().run_until_complete(
            _run_in_another_process(_grpc_aio_call)
        )
        self.assertEqual(successful_call, True)


    def test_insecure_channel(self):
        async def coro():
            channel = aio.insecure_channel('target:port')
            self.assertIsInstance(channel, aio.Channel)

        asyncio.get_event_loop().run_until_complete(coro())

if __name__ == '__main__':
    logging.basicConfig()
    unittest.main(verbosity=2)
