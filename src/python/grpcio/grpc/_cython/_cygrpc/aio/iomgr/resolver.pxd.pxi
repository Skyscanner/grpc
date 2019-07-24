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

cdef class _AsyncioResolver:
    cdef:
        grpc_custom_resolver* g_resolver
        object task_resolve
        char* host
        char* port

    @staticmethod
    cdef _AsyncioResolver create(grpc_custom_resolver* g_resolver)

    cdef void resolve(self, char* host, char* port)
