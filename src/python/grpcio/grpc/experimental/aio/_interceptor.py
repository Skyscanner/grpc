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
"""Interceptors implementation of gRPC Asyncio Python."""
import asyncio
import collections
import functools
from typing import Any, Dict, Callable, Optional, List, Iterator

import grpc
from grpc import _common
from grpc._cython import cygrpc

from . import _base_call
from ._call import UnaryUnaryCall
from ._utils import _timeout_to_deadline
from ._typing import RequestType, SerializingFunction, DeserializingFunction, MetadataType

_LOCAL_CANCELLATION_BEFORE_RPC_DETAILS = 'Locally cancelled by application before starting the RPC!'

class ClientCallDetails(
        collections.namedtuple(
            'ClientCallDetails',
            ('method', 'timeout', 'metadata', 'credentials')),
        grpc.ClientCallDetails):

    method: bytes
    timeout: Optional[float]
    metadata: Optional[Dict]
    credentials: Optional[Any]


class UnaryUnaryClientInterceptor:
    """Affords intercepting unary-unary invocations."""

    async def intercept_unary_unary(
            self, continuation: Callable[[ClientCallDetails, Any], Any],
            client_call_details: ClientCallDetails, request: Any) -> Any:
        """Intercepts a unary-unary invocation asynchronously.
        Args:
          continuation: A coroutine that proceeds with the invocation by
            executing the next interceptor in chain or invoking the
            actual RPC on the underlying Channel. It is the interceptor's
            responsibility to call it if it decides to move the RPC forward.
            The interceptor can use
            `response_future = await continuation(client_call_details, request)`
            to continue with the RPC. `continuation` returns the response of the
            RPC.
          client_call_details: A ClientCallDetails object describing the
            outgoing RPC.
          request: The request value for the RPC.
        Returns:
            An object with the RPC response.
        Raises:
          AioRpcError: Indicating that the RPC terminated with non-OK status.
          asyncio.CancelledError: Indicating that the RPC was canceled.
        """


class InterceptedUnaryUnaryCall(_base_call.UnaryUnaryCall):
    """An intercepted call used for proxy all of the calls to an intercepted
    call.

    It handles also early cancellatoins, when the RPC has not even started and
    the execution is still hold by the interceptors.

    Once the RPC is finnally executed, all methods are finnally done against
    the `UnaryUnaryCall`, which is at the same time the same call returned
    to the interceptors.
    
    For the `__await__` method is it is proxied to the intercepted call only
    when the interceptors task is finished.
    """

    _loop: asyncio.AbstractEventLoop
    _channel: cygrpc.AioChannel
    _cancelled_before_rpc: bool
    _intercepted_call: Optional[_base_call.UnaryUnaryCall]
    _intercepted_call_created: asyncio.Event
    _interceptors_task: asyncio.Task

    def __init__(self, interceptors: List[UnaryUnaryClientInterceptor],
            request: RequestType, timeout: Optional[float],
            channel: cygrpc.AioChannel, method: bytes,
            request_serializer: SerializingFunction,
            response_deserializer: DeserializingFunction) -> None:
        self._channel = channel
        self._loop = asyncio.get_event_loop()
        self._intercepted_call = None
        self._intercepted_call_created = asyncio.Event(loop=self._loop)
        self._cancelled_before_rpc = False
        self._invoke(interceptors, method, timeout, request, request_serializer, response_deserializer)

    def __del__(self):
        self.cancel()

    def _invoke(self, interceptors: List[UnaryUnaryClientInterceptor],
                method: bytes, timeout: Optional[float],
                request: RequestType,
                request_serializer: SerializingFunction,
                response_deserializer: DeserializingFunction) -> None:
        """Run the RPC call wraped in interceptors"""

        async def _run_interceptor(
                interceptors: Iterator[UnaryUnaryClientInterceptor],
                client_call_details: ClientCallDetails,
                request: RequestType) -> _base_call.UnaryUnaryCall:
            try:
                interceptor = next(interceptors)
            except StopIteration:
                interceptor = None

            if interceptor:
                continuation = functools.partial(_run_interceptor, interceptors)
                return await interceptor.intercept_unary_unary(
                    continuation, client_call_details, request)
            else:
                self._intercepted_call = UnaryUnaryCall(
                    request,
                    _timeout_to_deadline(self._loop, client_call_details.timeout),
                    self._channel,
                    client_call_details.method,
                    request_serializer,
                    response_deserializer
                )
                self._intercepted_call_created.set()
                return self._intercepted_call

        client_call_details = ClientCallDetails(
            method, timeout, None, None)

        self._interceptors_task = asyncio.ensure_future(
            _run_interceptor(iter(interceptors), client_call_details, request)
        )

    def cancel(self) -> bool:
        if self._cancelled_before_rpc:
            return False

        if not self._intercepted_call_created.is_set():
            self._cancelled_before_rpc = True
            self._interceptors_task.cancel()
            self._intercepted_call_created.set()
            return True

        status = self._intercepted_call.cancel()

        # If interceptors code are still running we
        # cancel them
        if not self._interceptors_task.done():
            self._interceptors_task.cancel()

        return status

    def cancelled(self) -> bool:
        if self._cancelled_before_rpc:
            return True

        if not self._intercepted_call_created.is_set():
            return False

        return self._intercepted_call.cancelled()

    def done(self) -> bool:
        if self._cancelled_before_rpc:
            return True

        if not self._intercepted_call_created.is_set():
            return False

        return self._intercepted_call.done()

    def add_done_callback(self, unused_callback) -> None:
        raise NotImplementedError()

    def time_remaining(self) -> Optional[float]:
        raise NotImplementedError()

    async def initial_metadata(self) -> Optional[MetadataType]:
        if self._cancelled_before_rpc:
            return None

        await self._intercepted_call_created.wait()

        # while was waiting for the initial metadata
        # the intercepted call was cancelled before
        # the RPC was invoked
        if self._cancelled_before_rpc:
            return None

        return await self._intercepted_call.initial_metadata()

    async def trailing_metadata(self) -> Optional[MetadataType]:
        if self._cancelled_before_rpc:
            return None

        await self._intercepted_call_created.wait()

        # while was waiting for the trailing metadata
        # the intercepted call was cancelled before
        # the RPC was invoked
        if self._cancelled_before_rpc:
            return None

        return await self._intercepted_call.trailing_metadata()

    async def code(self) -> grpc.StatusCode:
        if self._cancelled_before_rpc:
            return grpc.StatusCode.CANCELLED

        await self._intercepted_call_created.wait()

        # while was waiting for the code
        # the intercepted call was cancelled before
        # the RPC was invoked
        if self._cancelled_before_rpc:
            return grpc.StatusCode.CANCELLED

        return await self._intercepted_call.code()

    async def details(self) -> str:
        if self._cancelled_before_rpc:
            return _LOCAL_CANCELLATION_BEFORE_RPC_DETAILS

        await self._intercepted_call_created.wait()

        # while was waiting for the details
        # the intercepted call was cancelled before
        # the RPC was invoked
        if self._cancelled_before_rpc:
            return _LOCAL_CANCELLATION_BEFORE_RPC_DETAILS

        return await self._intercepted_call.details()

    async def debug_error_string(self) -> str:
        if self._cancelled_before_rpc:
            return _LOCAL_CANCELLATION_BEFORE_RPC_DETAILS

        await self._intercepted_call_created.wait()

        # while was waiting for the debug error string
        # the intercepted call was cancelled before
        # the RPC was invoked
        if self._cancelled_before_rpc:
            return _LOCAL_CANCELLATION_BEFORE_RPC_DETAILS

        return await self._intercepted_call.debug_error_string()

    def __await__(self):
        yield from self._interceptors_task.__await__()
        response = yield from self._intercepted_call.__await__()
        return response
