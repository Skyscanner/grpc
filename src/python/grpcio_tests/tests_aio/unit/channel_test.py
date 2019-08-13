import asyncio
import logging
import unittest

from unittest.mock import MagicMock, patch

from grpc.experimental import aio


class TestChannel(unittest.TestCase):
    def setUp(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    def test_unary_unary(self):
        async def coro():
            channel = aio.insecure_channel('target:port')
            self.assertIsInstance(
                channel.unary_unary('/grpc.testing.TestService/UnaryCall'),
                aio.UnaryUnaryMultiCallable
            )

        with patch("grpc._cython.cygrpc"):
            asyncio.get_event_loop().run_until_complete(coro())

    def test_unary_sets_serialisers(self):
        async def coro():
            channel = aio.insecure_channel('target:port')
            request_serializer = MagicMock()
            response_deserializer = MagicMock()
            multicallable = channel.unary_unary(
                '/grpc.testing.TestService/UnaryCall',
                request_serializer=request_serializer,
                response_deserializer=response_deserializer
            )

            self.assertIs(multicallable._request_serializer, request_serializer)
            self.assertIs(multicallable._response_deserializer, response_deserializer)

        with patch("grpc._cython.cygrpc"):
            asyncio.get_event_loop().run_until_complete(coro())



class TestUnaryUnaryMultiCallable(unittest.TestCase):
    def test_call(self):
        async def coro():
            channel = aio.insecure_channel('target:port')
            multicallable = channel.unary_unary('/grpc.testing.TestService/UnaryCall')
            multicallable._channel = MagicMock()
            async def unary_unary(*args, **kwargs):
                return 'world'
            multicallable._channel.unary_unary = MagicMock(wraps=unary_unary)

            response = await multicallable(b'hello')
            self.assertEqual(response, 'world')
            multicallable._channel.unary_unary.assert_called_once_with(b'/grpc.testing.TestService/UnaryCall', b'hello')

        with patch("grpc._cython.cygrpc"):
            asyncio.get_event_loop().run_until_complete(coro())

    def test_call_serializes_request(self):
        async def coro():
            channel = aio.insecure_channel('target:port')
            request_serializer = MagicMock(return_value='serialized_hello')
            multicallable = channel.unary_unary(
                '/grpc.testing.TestService/UnaryCall',
                request_serializer=request_serializer
            )
            multicallable._channel = MagicMock()
            async def unary_unary(*args, **kwargs):
                return 'world'
            multicallable._channel.unary_unary = MagicMock(wraps=unary_unary)

            await multicallable(b'hello')
            multicallable._channel.unary_unary.assert_called_once_with(
                b'/grpc.testing.TestService/UnaryCall', 'serialized_hello')

        with patch("grpc._cython.cygrpc"):
            asyncio.get_event_loop().run_until_complete(coro())


    def test_call_deserializes_response(self):
        async def coro():
            channel = aio.insecure_channel('target:port')
            response_deserializer = MagicMock(return_value='deserialized_world')
            multicallable = channel.unary_unary(
                '/grpc.testing.TestService/UnaryCall',
                response_deserializer=response_deserializer
            )
            multicallable._channel = MagicMock()
            async def unary_unary(*args, **kwargs):
                return 'world'
            multicallable._channel.unary_unary = MagicMock(wraps=unary_unary)

            response = await multicallable(b'hello')
            self.assertEqual(response, 'deserialized_world')

        with patch("grpc._cython.cygrpc"):
            asyncio.get_event_loop().run_until_complete(coro())

if __name__ == '__main__':
    logging.basicConfig()
    unittest.main(verbosity=2)
