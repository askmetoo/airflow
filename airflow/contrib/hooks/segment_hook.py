# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
This module contains a Salesforce Hook
which allows you to connect to your Salesforce instance,
retrieve data from it, and write that data to a file
for other uses.

NOTE:   this hook also relies on the simple_salesforce package:
        https://github.com/simple-salesforce/simple-salesforce
"""
import analytics
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException

from airflow.utils.log.logging_mixin import LoggingMixin


class SegmentHook(BaseHook, LoggingMixin):
    def __init__(
            self,
            segment_conn_id,
            *args,
            **kwargs
    ):
        """
        Create new connection to Salesforce
        and allows you to pull data out of SFDC and save it to a file.

        You can then use that file with other
        Airflow operators to move the data into another data source

        :param segment_conn_id: the name of the connection that has the parameters
                            we need to connect to Salesforce.
                            The conenction shoud be type `http` and include a
                            user's security token in the `Extras` field.
        .. note::
            For the HTTP connection type, you can include a
            JSON structure in the `Extras` field.
            We need a user's security token to connect to Salesforce.
            So we define it in the `Extras` field as:
                `{"security_token":"YOUR_SECRUITY_TOKEN"}`
        """
        self.segment_conn_id = segment_conn_id
        self._args = args
        self._kwargs = kwargs

        # get the connection parameters
        self.connection = self.get_connection(self.segment_conn_id)
        self.extras = self.connection.extra_dejson
        self.write_key = self.extras.get('write_key')
        if not self.write_key:
            raise AirflowException('No Segment write key provided')

    def get_conn(self):
        self.log.debug('Setting write key for Segment analytics connection')
        analytics.debug = True
        analytics.on_error = self.on_error
        analytics.write_key = self.write_key
        return analytics

    # def get_records(self, sql):
    #     raise NotImplementedError()
    #
    # def get_pandas_df(self, sql):
    #     raise NotImplementedError()
    #
    # def run(self, sql):
    #     raise NotImplementedError()

    def on_error(self, error, items):
        self.log.error('Encountered Segment error: {segment_error} with items: {with_items}'.format(segment_error=error, with_items=items))
        raise AirflowException('Segment error: {}'.format(error))
