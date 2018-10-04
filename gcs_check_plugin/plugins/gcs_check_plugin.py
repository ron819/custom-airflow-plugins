# -*- coding: utf-8 -*-
#
# Copyright (c) John Lewis & Partners
#
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

"""
This contains a plugin which checks if a file exists on google cloud storage
"""
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator, SkipMixin
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin


class CheckGCSObjectExistsOperator(BaseOperator):
    """
    Checks a file exists on Google Cloud Storage.

    :param bucket: The Google cloud storage bucket where the object is.
    :type bucket: string
    :param object_to_check: The full path in the bucket to chkce if object exists
    :type object_to_check: string
    :param store_to_xcom_key: If this param is set, the operator will push
        whether the file exists to this xcom key.
        If it does not exist, whether a file exists or not will be pushed to the xcom key object_exists
    :type store_to_xcom_key: string
    :param google_cloud_storage_conn_id: The connection ID to use when
        connecting to Google cloud storage.
    :type google_cloud_storage_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide delegation enabled.
    :type delegate_to: string
    """
    template_fields = ('bucket', 'object_to_check', 'store_to_xcom_key',)
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 bucket,
                 object_to_check,
                 store_to_xcom_key=False,
                 google_cloud_storage_conn_id='google_cloud_storage_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(CheckGCSObjectExistsOperator, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.object_to_check = object_to_check
        self.store_to_xcom_key = store_to_xcom_key
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        self.log.info('Checking exists: %s, %s', self.bucket, self.object_to_check)
        hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
                                      delegate_to=self.delegate_to)
        object_exists = hook.exists(self.bucket, self.object_to_check)
        if self.store_to_xcom_key:
            context['ti'].xcom_push(key=self.store_to_xcom_key, value=object_exists)
        else:
            context['ti'].xcom_push(key='object_exists', value=object_exists)
        self.log.info(object_exists)

class BranchGCSObjectExistsOperator(BaseOperator, SkipMixin):
    """
    Checks a file exists on Google Cloud Storage and branches on which task to run next
    depending on whether object exist.
    This is similar to the branch python operator.

    :param bucket: The Google cloud storage bucket where the object is.
    :type bucket: string
    :param object_to_check: The full path in the bucket to chkce if object exists
    :type object_to_check: string
    :param exist_task: The task to run next if object exists
    :type exist_task: string
    :param not_exist_task: The task to run next if object does not exist
    :type not_exist_task: string
    :param google_cloud_storage_conn_id: The connection ID to use when
        connecting to Google cloud storage.
    :type google_cloud_storage_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide delegation enabled.
    :type delegate_to: string
    """
    template_fields = ('bucket', 'object_to_check',)
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 bucket,
                 object_to_check,
                 exist_task=None,
                 not_exist_task=None,
                 google_cloud_storage_conn_id='google_cloud_storage_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(BranchGCSObjectExistsOperator, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.object_to_check = object_to_check
        self.exist_task = exist_task
        self.not_exist_task = not_exist_task
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        self.log.info('Checking exists: %s, %s', self.bucket, self.object_to_check)
        hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
                                      delegate_to=self.delegate_to)
        object_exists = hook.exists(self.bucket, self.object_to_check)
        self.log.info('Object exists : %s', object_exists)
        branch = self.not_exist_task
        if object_exists:
            branch = self.exist_task
        # set tasks in the unselected task list to be skipped
        downstream_tasks = context['task'].downstream_list
        self.log.info('Following branch %s', branch)
        self.log.info('Downstream task_ids %s', downstream_tasks)
        skip_tasks = [t for t in downstream_tasks if t.task_id != branch]
        if downstream_tasks:
            self.skip(context['dag_run'], context['ti'].execution_date, skip_tasks)

class GCSCheckPlugin(AirflowPlugin):
    """
    define the plugin class
    """
    name = 'gcs_check_plugin'
    operators = [CheckGCSObjectExistsOperator, BranchGCSObjectExistsOperator]
