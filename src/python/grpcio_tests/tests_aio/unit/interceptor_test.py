# Copyright 2019 The gRPC Authors.
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
import collections
import logging
import unittest

import grpc

from grpc.experimental import aio
from tests_aio.unit._test_server import start_test_server
from tests_aio.unit._test_base import AioTestBase
from src.proto.grpc.testing import messages_pb2


class TestUnaryUnaryClientInterceptor(AioTestBase):

    def test_invalid(self):

        class InvalidInterceptor:
            """Just an invalid Interceptor"""

        with self.assertRaises(ValueError):
            aio.insecure_channel("", interceptors=[InvalidInterceptor()])

    async def test_executed_right_order(self):

        interceptors_executed = []

        class Interceptor(aio.UnaryUnaryClientInterceptor):
            """Interceptor used for testing if the interceptor is being called"""

            async def intercept_unary_unary(self, continuation,
                                            client_call_details, request):
                interceptors_executed.append(self)
                call = await continuation(client_call_details, request)
                return call

        interceptors = [Interceptor() for i in range(2)]

        server_target, _ = await start_test_server()  # pylint: disable=unused-variable

        async with aio.insecure_channel(
                server_target, interceptors=interceptors) as channel:
            multicallable = channel.unary_unary(
                '/grpc.testing.TestService/UnaryCall',
                request_serializer=messages_pb2.SimpleRequest.
                SerializeToString,
                response_deserializer=messages_pb2.SimpleResponse.FromString
            )
            call = multicallable(messages_pb2.SimpleRequest())
            response = await call

            # Check that all interceptors were executed, and were executed
            # in the right order.
            self.assertEqual(interceptors_executed, interceptors)

            self.assertEqual(type(response), messages_pb2.SimpleResponse)

    @unittest.expectedFailure
    # TODO(https://github.com/grpc/grpc/issues/20144) Once metadata support is
    # implemented in the client-side, this test must be implemented.
    def test_modify_metadata(self):
        raise NotImplementedError()

    @unittest.expectedFailure
    # TODO(https://github.com/grpc/grpc/issues/20532) Once credentials support is
    # implemented in the client-side, this test must be implemented.
    def test_modify_credentials(self):
        raise NotImplementedError()

    async def test_status_code_Ok(self):

        class StatusCodeOkInterceptor(
                aio.UnaryUnaryClientInterceptor):
            """Interceptor used for observe status code Ok returned by the RPC"""

            def __init__(self):
                self.status_code_Ok_observed = False 

            async def intercept_unary_unary(self, continuation,
                                            client_call_details, request):
                call = await continuation(client_call_details, request)
                code = await call.code()
                if (code == grpc.StatusCode.OK):
                    self.status_code_Ok_observed = True

                return call

        interceptor = StatusCodeOkInterceptor()
        server_target, server = await start_test_server()

        async with aio.insecure_channel(
                server_target, interceptors=[interceptor]) as channel:

            # when no error StatusCode.OK must be observed
            multicallable = channel.unary_unary(
                '/grpc.testing.TestService/UnaryCall',
                request_serializer=messages_pb2.SimpleRequest.
                SerializeToString,
                response_deserializer=messages_pb2.SimpleResponse.FromString
            )

            await multicallable(messages_pb2.SimpleRequest())

            self.assertTrue(
                interceptor.status_code_Ok_observed)

    async def test_add_timeout(self):

        class TimeoutInterceptor(aio.UnaryUnaryClientInterceptor):
            """Interceptor used for adding a timeout to the RPC"""

            async def intercept_unary_unary(self, continuation,
                                            client_call_details, request):
                new_client_call_details = aio.ClientCallDetails(
                    method=client_call_details.method,
                    timeout=0.1,
                    metadata=client_call_details.metadata,
                    credentials=client_call_details.credentials)
                return await continuation(new_client_call_details, request)

        interceptor = TimeoutInterceptor()
        server_target, server = await start_test_server()

        async with aio.insecure_channel(
                server_target, interceptors=[interceptor]) as channel:

            multicallable = channel.unary_unary(
                '/grpc.testing.TestService/UnaryCall',
                request_serializer=messages_pb2.SimpleRequest.
                SerializeToString,
                response_deserializer=messages_pb2.SimpleResponse.FromString
            )

            await server.stop(None)

            with self.assertRaises(grpc.RpcError) as exception_context:
                await multicallable(messages_pb2.SimpleRequest())

            self.assertEqual(exception_context.exception.code(),
                             grpc.StatusCode.DEADLINE_EXCEEDED)


class TestInterceptedUnaryUnaryCall(AioTestBase):

    async def test_call_ok(self):

        class Interceptor(aio.UnaryUnaryClientInterceptor):

            async def intercept_unary_unary(self, continuation,
                                            client_call_details, request):
                call = await continuation(client_call_details, request)
                return call


        server_target, _ = await start_test_server()  # pylint: disable=unused-variable

        async with aio.insecure_channel(
                server_target, interceptors=[Interceptor()]) as channel:

            multicallable = channel.unary_unary(
                '/grpc.testing.TestService/UnaryCall',
                request_serializer=messages_pb2.SimpleRequest.
                SerializeToString,
                response_deserializer=messages_pb2.SimpleResponse.FromString
            )
            call = multicallable(messages_pb2.SimpleRequest())
            response = await call

            self.assertTrue(call.done())
            self.assertFalse(call.cancelled())
            self.assertEqual(type(response), messages_pb2.SimpleResponse)
            self.assertEqual(await call.code(), grpc.StatusCode.OK)
            self.assertEqual(await call.details(), '')
            self.assertEqual(await call.initial_metadata(), ())
            self.assertEqual(await call.trailing_metadata(), ())


    async def test_cancel_before_rpc(self):

        interceptor_reached = asyncio.Event()

        class Interceptor(aio.UnaryUnaryClientInterceptor):

            async def intercept_unary_unary(self, continuation,
                                            client_call_details, request):
                interceptor_reached.set()
                await asyncio.sleep(0)

                # This line should never be reached
                raise Exception()


        server_target, _ = await start_test_server()  # pylint: disable=unused-variable

        async with aio.insecure_channel(
                server_target, interceptors=[Interceptor()]) as channel:

            multicallable = channel.unary_unary(
                '/grpc.testing.TestService/UnaryCall',
                request_serializer=messages_pb2.SimpleRequest.
                SerializeToString,
                response_deserializer=messages_pb2.SimpleResponse.FromString
            )
            call = multicallable(messages_pb2.SimpleRequest())

            self.assertFalse(call.cancelled())
            self.assertFalse(call.done())

            await interceptor_reached.wait()
            self.assertTrue(call.cancel())
            self.assertFalse(call.cancel())

            with self.assertRaises(asyncio.CancelledError):
                await call

            self.assertTrue(call.cancelled())
            self.assertTrue(call.done())
            self.assertEqual(await call.code(), grpc.StatusCode.CANCELLED)
            self.assertEqual(await call.details(), 'Locally cancelled by application before starting the RPC!')
            self.assertEqual(await call.initial_metadata(), None)
            self.assertEqual(await call.trailing_metadata(), None)

    async def test_cancel_after_rpc(self):

        interceptor_reached = asyncio.Event()

        class Interceptor(aio.UnaryUnaryClientInterceptor):

            async def intercept_unary_unary(self, continuation,
                                            client_call_details, request):
                call = await continuation(client_call_details, request)
                await call
                interceptor_reached.set()
                await asyncio.sleep(0)

                # This line should never be reached
                raise Exception()


        server_target, _ = await start_test_server()  # pylint: disable=unused-variable

        async with aio.insecure_channel(
                server_target, interceptors=[Interceptor()]) as channel:

            multicallable = channel.unary_unary(
                '/grpc.testing.TestService/UnaryCall',
                request_serializer=messages_pb2.SimpleRequest.
                SerializeToString,
                response_deserializer=messages_pb2.SimpleResponse.FromString
            )
            call = multicallable(messages_pb2.SimpleRequest())

            self.assertFalse(call.cancelled())
            self.assertFalse(call.done())

            await interceptor_reached.wait()

            self.assertFalse(call.cancel())

            with self.assertRaises(asyncio.CancelledError):
                await call

            self.assertFalse(call.cancelled())
            self.assertTrue(call.done())
            self.assertEqual(await call.code(), grpc.StatusCode.OK)
            self.assertEqual(await call.details(), '')
            self.assertEqual(await call.initial_metadata(), ())
            self.assertEqual(await call.trailing_metadata(), ())


if __name__ == '__main__':
    logging.basicConfig()
    unittest.main(verbosity=2)
