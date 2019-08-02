# Copyright 2016 gRPC authors.
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

import json
import pkgutil
import unittest
import sys

import six

import tests


class SanityTest(unittest.TestCase):

    maxDiff = 32768

    def testTestsJsonUpToDate(self):
        """Autodiscovers all test suites and checks that tests.json is up to date"""
        loader = tests.Loader()
        loader.loadTestsFromNames(['tests'])
        test_suite_names = sorted({
            test_case_class.id().rsplit('.', 1)[0]
            for test_case_class in tests._loader.iterate_suite_cases(
                loader.suite)
        })

        tests_json_string = pkgutil.get_data('tests', 'tests.json')
        tests_json = json.loads(tests_json_string.decode()
                                if six.PY3 else tests_json_string)

        asyncio_tests_json = []
        if sys.version_info >= (3, 6):
          asyncio_tests_json_string = pkgutil.get_data('tests',
                                                       'asyncio_tests.json')
          asyncio_tests_json = json.loads(asyncio_tests_json_string.decode()
                                          if six.PY3 else asyncio_tests_json_string)

        self.assertSequenceEqual(tests_json, sorted(tests_json))
        self.assertSequenceEqual(asyncio_tests_json, sorted(asyncio_tests_json))
        self.assertSequenceEqual(sorted(tests_json + asyncio_tests_json), test_suite_names)


if __name__ == '__main__':
    unittest.main(verbosity=2)
