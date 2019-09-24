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

import argparse

from concurrent import futures

import grpc
from src.proto.grpc.testing import messages_pb2
from src.proto.grpc.testing import test_pb2_grpc


_INITIAL_METADATA_KEY = "initial-md-key"
_TRAILING_METADATA_KEY = "trailing-md-key-bin"


def _maybe_echo_metadata(servicer_context):
    """Copies metadata from request to response if it is present."""
    invocation_metadata = dict(servicer_context.invocation_metadata())
    if _INITIAL_METADATA_KEY in invocation_metadata:
        initial_metadatum = (_INITIAL_METADATA_KEY,
                             invocation_metadata[_INITIAL_METADATA_KEY])
        servicer_context.send_initial_metadata((initial_metadatum,))
    if _TRAILING_METADATA_KEY in invocation_metadata:
        trailing_metadatum = (_TRAILING_METADATA_KEY,
                              invocation_metadata[_TRAILING_METADATA_KEY])
        servicer_context.set_trailing_metadata((trailing_metadatum,))


# TODO (https://github.com/grpc/grpc/issues/19762)
# Change for an asynchronous server version once it's implemented.
class TestServiceServicer(test_pb2_grpc.TestServiceServicer):

    def UnaryCall(self, request, context):
        _maybe_echo_metadata(context)
        return messages_pb2.SimpleResponse()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Synchronous gRPC server.')
    parser.add_argument(
        '--host_and_port',
        required=True,
        type=str,
        nargs=1,
        help='the host and port to listen.')
    args = parser.parse_args()

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=1),
        options=(('grpc.so_reuseport', 1),))
    test_pb2_grpc.add_TestServiceServicer_to_server(TestServiceServicer(),
                                                    server)
    server.add_insecure_port(args.host_and_port[0])
    server.start()
    server.wait_for_termination()
