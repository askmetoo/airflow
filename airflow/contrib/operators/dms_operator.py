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


class DMSOperator(BaseOperator):
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
    # template_fields = []

    @apply_defaults
    def __init__(self, replication_task_arn, start_replication_task_type, cdc_start_time=None,
                 aws_conn_id=None, region_name=None, **kwargs):
        super(DMSOperator, self).__init__(**kwargs)

        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.replication_task_arn = replication_task_arn
        self.start_replication_task_type = start_replication_task_type
        self.cdc_start_time = cdc_start_time

        self.hook = self.get_hook()

    def execute(self, context):
        self.log.info(
            'Running DMS Task - Replication task arn: %s - with replication task type %s',
            self.replication_task_arn,self.start_replication_task_type
        )
        self.log.info('DMSOperator CDC start time: %s', self.cdc_start_time)

        self.client = self.hook.get_client_type(
            'dms',
            region_name=self.region_name
        )

        response = self.client.start_replication_task(
            ReplicationTaskArn=self.replication_task_arn,
            StartReplicationTaskType=self.start_replication_task_type
            # ,CdcStartTime=self.cdc_start_time,
        )

        # failures = response['failures']
        # if len(failures) > 0:
        #     raise AirflowException(response)
        self.log.info('DMS Task started: %s', response)
        #
        # self.arn = response['tasks'][0]['taskArn']
        self.replication_task_id = response['ReplicationTask']['ReplicationTaskIdentifier']
        self._wait_for_task_ended()

        self._check_success_task()
        self.log.info('DMS Task has been successfully executed: %s', response)

    def _wait_for_task_ended(self):
        print("dms waiters:")
        print(self.client.waiter_names)
        self.log.info('DMS waiters: %s', self.client.waiter_names)
        waiter = self.client.get_waiter('dms_tasks_stopped')
        waiter.config.max_attempts = sys.maxsize  # timeout is managed by airflow
        waiter.wait(
            cluster=self.cluster,
            tasks=[self.arn]
        )

    def _check_success_task(self):
        response = self.client.describe_replication_tasks(
            Filters=[
                {
                    'Name': 'replication-task-id',
                    'Values': [
                        self.replication_task_id,
                    ]
                },
            ]
        )
        self.log.info('DMS Task stopped, check status: %s', response)

        # if len(response.get('failures', [])) > 0:
        #     raise AirflowException(response)
        #
        # for task in response['tasks']:
        #     containers = task['containers']
        #     for container in containers:
        #         if container.get('lastStatus') == 'STOPPED' and container['exitCode'] != 0:
        #             raise AirflowException('This task is not in success state {}'.format(task))
        #         elif container.get('lastStatus') == 'PENDING':
        #             raise AirflowException('This task is still pending {}'.format(task))
        #         elif 'error' in container.get('reason', '').lower():
        #             raise AirflowException('This containers encounter an error during launching : {}'.
        #                                    format(container.get('reason', '').lower()))

    def get_hook(self):
        return AwsHook(
            aws_conn_id=self.aws_conn_id
        )

    def on_kill(self):
        response = self.client.stop_replication_task(
            ReplicationTaskArn=self.replication_task_arn
        )
        self.log.info('DMS on_kill: %s', response)
