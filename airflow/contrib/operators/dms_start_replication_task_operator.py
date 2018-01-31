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
import sys

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils import apply_defaults

from airflow.contrib.hooks.aws_hook import AwsHook


class DMSStartReplicationTaskOperator(BaseOperator):
    """
    Execute a task on AWS EC2 Container Service

    :param task_definition: the task definition name on EC2 Container Service
    :type task_definition: str
    :param cluster: the cluster name on EC2 Container Service
    :type cluster: str
    :param: overrides: the same parameter that boto3 will receive:
            http://boto3.readthedocs.org/en/latest/reference/services/ecs.html#ECS.Client.run_task
    :type: overrides: dict
    :param aws_conn_id: connection id of AWS credentials / region name. If None,
            credential boto3 strategy will be used (http://boto3.readthedocs.io/en/latest/guide/configuration.html).
    :type aws_conn_id: str
    :param region_name: region name to use in AWS Hook. Override the region_name in connection (if provided)
    """

    ui_color = '#f0ede4'
    client = None
    arn = None
    template_fields = ['replication_task_arn']

    @apply_defaults
    def __init__(self, replication_task_arn, start_replication_task_type, cdc_start_time=None,
                 aws_conn_id=None, region_name=None, **kwargs):
        super(DMSStartReplicationTaskOperator, self).__init__(**kwargs)

        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.replication_task_arn = replication_task_arn
        self.start_replication_task_type = start_replication_task_type
        self.cdc_start_time = cdc_start_time

        self.hook = self.get_hook()

    def execute(self, context):
        self.log.info('Starting DMS Start Replication Task')
        self.client = self.hook.get_client_type(
            'ecs',
            region_name=self.region_name
        )
        response = self.client.start_replication_task(
            ReplicationTaskArn=self.replication_task_arn,
            StartReplicationTaskType=self.start_replication_task_type
            # ,CdcStartTime=self.cdc_start_time,
        )

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException('Adding steps failed: %s' % response)
        else:
            self.log.info('Running Delete DMS Task')
            return response['ReplicationTask']['ReplicationTaskArn']

    def get_hook(self):
        return AwsHook(
            aws_conn_id=self.aws_conn_id
        )

    def on_kill(self):
        response = self.client.stop_replication_task(
            ReplicationTaskArn=self.replication_task_arn
        )
        self.log.info('Create DMS on_kill: %s', response)
