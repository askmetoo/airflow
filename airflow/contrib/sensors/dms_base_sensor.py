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
from airflow.operators.sensors import BaseSensorOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils import apply_defaults
from airflow.exceptions import AirflowException


class DMSBaseSensor(BaseSensorOperator):
    """
    Contains general sensor behavior for EMR.
    Subclasses should implement get_emr_response() and state_from_response() methods.
    Subclasses should also implement NON_TERMINAL_STATES and FAILED_STATE constants.
    """
    ui_color = '#66c3ff'
    template_fields = ['replication_task_arn']
    template_ext = ()

    @apply_defaults
    def __init__(
            self,
            replication_task_arn,
            aws_conn_id='aws_default',
            region_name=None,
            *args, **kwargs):
        super(DMSBaseSensor, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.replication_task_arn = replication_task_arn

        # self.log.info('Starting DMS Delete Replication Task')
        # self.client = self.hook.get_client_type(
        #     'dms',
        #     region_name=self.region_name
        # )
        # response = self.client.create_replication_task(
        #     ReplicationTaskIdentifier=self.replication_task_identifier,
        #     SourceEndpointArn=self.source_endpoint_arn,
        #     TargetEndpointArn=self.target_endpoint_arn,
        #     ReplicationInstanceArn=self.replication_instance_arn,
        #     MigrationType=self.migration_type,
        #     TableMappings=self.table_mappings,
        #     ReplicationTaskSettings=self.replication_task_settings
        # )
        #
        # if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
        #     raise AirflowException('Creating DMS failed: %s' % response)
        # else:
        #     self.log.info('Running Create DMS Task with arn: %s', response['ReplicationTask']['ReplicationTaskArn'])
        #     return response['ReplicationTask']['ReplicationTaskArn']

    def poke(self, context):
        response = self.get_dms_response()

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            self.log.info('Bad HTTP response: %s', response)
            return False

        status = self.status_from_response(response)
        self.log.info('DMS Replication task currently %s', status)

        if status in self.NON_TERMINAL_STATUSES:
            return False

        if status in self.FAILED_STATUSES:
            raise AirflowException('DMS replication task failed')

        return True

    def get_dms_response(self):
        self.log.info('Poking replication task %s', self.replication_task_arn)
        return self.get_client().describe_replication_tasks(
            Filters=[
                {
                    'Name': 'replication-task-arn',
                    'Values': [
                        self.replication_task_arn,
                        ]
                },
            ],
        )

    def stop_reason_handling(self, stop_reason):
        pass

    def status_from_response(self, response):
        replication_task = response['ReplicationTasks'][0]
        if 'StopReason' in replication_task:
            stop_reason = replication_task['StopReason']
            self.stop_reason_handling(stop_reason)
            self.log.info(
                'Replication task: %s stopped with StopReason: %s', self.replication_task_arn, stop_reason
            )
        return replication_task['Status']

    def get_client(self):
        return AwsHook(aws_conn_id=self.aws_conn_id).get_client_type(
            'dms',
            region_name=self.region_name
        )
