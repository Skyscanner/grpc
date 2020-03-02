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


cdef class CallbackFailureHandler:

    def __cinit__(self,
                  str core_function_name,
                  object error_details,
                  object exception_type):
        """Handles failure by raising exception."""
        self._core_function_name = core_function_name
        self._error_details = error_details
        self._exception_type = exception_type

    cdef handle(self, object future, object future_loop):
        exception = self._exception_type(
            'Failed "%s": %s' % (self._core_function_name, self._error_details)
        )

        if future_loop == _current_io_loop().asyncio_loop():
            future.set_exception(exception)
        else:
            def next_loop_iteration():
                future_loop.call_soon_threadsafe(future.set_exception, exception)

            asyncio.get_event_loop().call_soon(next_loop_iteration)


cdef class CallbackWrapper:

    def __cinit__(self, object future, object loop, CallbackFailureHandler failure_handler):
        self.context.functor.functor_run = self.functor_run
        self.context.waiter = <cpython.PyObject*>future
        self.context.waiter_loop = <cpython.PyObject*>loop
        self.context.failure_handler = <cpython.PyObject*>failure_handler
        self.context.callback_wrapper = <cpython.PyObject*>self
        # NOTE(lidiz) Not using a list here, because this class is critical in
        # data path. We should make it as efficient as possible.
        self._reference_of_future = future
        self._reference_of_loop = loop
        self._reference_of_failure_handler = failure_handler
        # NOTE(lidiz) We need to ensure when Core invokes our callback, the
        # callback function itself is not deallocated. Othersise, we will get
        # a segfault. We can view this as Core holding a ref.
        cpython.Py_INCREF(self)

    @staticmethod
    cdef void functor_run(
            grpc_experimental_completion_queue_functor* functor,
            int success) with gil:
        cdef CallbackContext *context = <CallbackContext *>functor
        cdef object waiter = <object>context.waiter
        cdef object waiter_loop = <object>context.waiter_loop
        if waiter.cancelled():
            return

        if success == 0:
            (<CallbackFailureHandler>context.failure_handler).handle(waiter, waiter_loop)
        else:
            if waiter_loop == _current_io_loop().asyncio_loop():
                waiter.set_result(None)
            else:
                def next_loop_iteration():
                    waiter_loop.call_soon_threadsafe(waiter.set_result, None)

                asyncio.get_event_loop().call_soon(next_loop_iteration)
        cpython.Py_DECREF(<object>context.callback_wrapper)

    cdef grpc_experimental_completion_queue_functor *c_functor(self):
        return &self.context.functor


cdef CallbackFailureHandler CQ_SHUTDOWN_FAILURE_HANDLER = CallbackFailureHandler(
    'grpc_completion_queue_shutdown',
    'Unknown',
    InternalError)


cdef class CallbackCompletionQueue:

    def __cinit__(self):
        loop = asyncio.get_event_loop()
        self._shutdown_completed = loop.create_future()
        self._wrapper = CallbackWrapper(
            self._shutdown_completed,
            loop,
            CQ_SHUTDOWN_FAILURE_HANDLER)
        self._cq = grpc_completion_queue_create_for_callback(
            self._wrapper.c_functor(),
            NULL
        )

    cdef grpc_completion_queue* c_ptr(self):
        return self._cq

    async def shutdown(self):
        grpc_completion_queue_shutdown(self._cq)
        await self._shutdown_completed
        grpc_completion_queue_destroy(self._cq)


class ExecuteBatchError(Exception): pass


async def execute_batch(GrpcCallWrapper grpc_call_wrapper,
                               tuple operations,
                               object loop):
    """The callback version of start batch operations."""
    cdef _BatchOperationTag batch_operation_tag = _BatchOperationTag(None, operations, None)
    batch_operation_tag.prepare()

    cdef object future = loop.create_future()
    cdef CallbackWrapper wrapper = CallbackWrapper(
        future,
        loop,
        CallbackFailureHandler('execute_batch', operations, ExecuteBatchError))
    cdef grpc_call_error error = grpc_call_start_batch(
        grpc_call_wrapper.call,
        batch_operation_tag.c_ops,
        batch_operation_tag.c_nops,
        wrapper.c_functor(), NULL)

    if error != GRPC_CALL_OK:
        raise ExecuteBatchError("Failed grpc_call_start_batch: {}".format(error))

    await future

    cdef grpc_event c_event
    # Tag.event must be called, otherwise messages won't be parsed from C
    batch_operation_tag.event(c_event)


cdef prepend_send_initial_metadata_op(tuple ops, tuple metadata):
    # Eventually, this function should be the only function that produces
    # SendInitialMetadataOperation. So we have more control over the flag.
    return (SendInitialMetadataOperation(
        metadata,
        _EMPTY_FLAG
    ),) + ops


async def _receive_message(GrpcCallWrapper grpc_call_wrapper,
                           object loop):
    """Retrives parsed messages from Core.

    The messages maybe already in Core's buffer, so there isn't a 1-to-1
    mapping between this and the underlying "socket.read()". Also, eventually,
    this function will end with an EOF, which reads empty message.
    """
    cdef ReceiveMessageOperation receive_op = ReceiveMessageOperation(_EMPTY_FLAG)
    cdef tuple ops = (receive_op,)
    try:
        await execute_batch(grpc_call_wrapper, ops, loop)
    except ExecuteBatchError as e:
        # NOTE(lidiz) The receive message operation has two ways to indicate
        # finish state : 1) returns empty message due to EOF; 2) fails inside
        # the callback (e.g. cancelled).
        #
        # Since they all indicates finish, they are better be merged.
        _LOGGER.debug(e)
    return receive_op.message()


async def _send_message(GrpcCallWrapper grpc_call_wrapper,
                        bytes message,
                        Operation send_initial_metadata_op,
                        int write_flag,
                        object loop):
    cdef SendMessageOperation op = SendMessageOperation(message, write_flag)
    cdef tuple ops = (op,)
    if send_initial_metadata_op is not None:
        ops = (send_initial_metadata_op,) + ops
    await execute_batch(grpc_call_wrapper, ops, loop)


async def _send_initial_metadata(GrpcCallWrapper grpc_call_wrapper,
                                 tuple metadata,
                                 int flags,
                                 object loop):
    cdef SendInitialMetadataOperation op = SendInitialMetadataOperation(
        metadata,
        flags)
    cdef tuple ops = (op,)
    await execute_batch(grpc_call_wrapper, ops, loop)


async def _receive_initial_metadata(GrpcCallWrapper grpc_call_wrapper,
                                    object loop):
    cdef ReceiveInitialMetadataOperation op = ReceiveInitialMetadataOperation(_EMPTY_FLAGS)
    cdef tuple ops = (op,)
    await execute_batch(grpc_call_wrapper, ops, loop)
    return op.initial_metadata()

async def _send_error_status_from_server(GrpcCallWrapper grpc_call_wrapper,
                                         grpc_status_code code,
                                         str details,
                                         tuple trailing_metadata,
                                         Operation send_initial_metadata_op,
                                         object loop):
    assert code != StatusCode.ok, 'Expecting non-ok status code.'
    cdef SendStatusFromServerOperation op = SendStatusFromServerOperation(
        trailing_metadata,
        code,
        details,
        _EMPTY_FLAGS,
    )
    cdef tuple ops = (op,)
    if send_initial_metadata_op is not None:
        ops = (send_initial_metadata_op,) + ops
    await execute_batch(grpc_call_wrapper, ops, loop)
