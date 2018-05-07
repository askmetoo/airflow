# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import unittest

from airflow.contrib.operators.segment_track_event_operator import \
    SegmentTrackEventOperator

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

TASK_ID = 'test-segment-track-event-operator'
USER_ID = '12345'
PROPERTIES = { 'foo': 'bar' }
EVENT = 'Test'


class SegmentTrackEventOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.segment_track_event_operator.SegmentHook')
    def test_execute(self, mock_hook):

        operator = SegmentTrackEventOperator(
            task_id=TASK_ID, user_id=USER_ID, event=EVENT, properties=PROPERTIES)

        operator.execute(None)

        mock_hook.return_value.track.assert_called_once_with(
            user_id=USER_ID, event=EVENT, properties=PROPERTIES)


if __name__ == '__main__':
    unittest.main()
