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
This contains one plugin which copies file from sftp site to a gcs bucket
"""
from tempfile import NamedTemporaryFile

from airflow.models import BaseOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.exceptions import AirflowException
from airflow.plugins_manager import AirflowPlugin

class SFTPToGCSOperator(BaseOperator):
    """
    Copies objects from an sftp host to a GCS bucket

    :param ssh_hook: predefined ssh_hook to use for remote execution
    :type ssh_hook: :class:`SSHHook`
    :param ssh_conn_id: connection id from airflow Connections
    :type ssh_conn_id: str
    :param remote_host: remote host to connect
    :type remote_host: str
    :param remote_filepath: remote file path to get (templated)
    :type remote_filepath: str
    :param destination_gcs_bucket: The destination Google Cloud Storage bucket (templated)
    :type destination_gcs_bucket: string
    :param destination_gcs_path: The destination Google Cloud Storage bucket (templated)
    :type destination_gcs_path: string
    :param google_cloud_storage_conn_id: The connection ID to use when
        connecting to Google cloud storage.
    :type google_cloud_storage_conn_id: string
    :type delegate_to: string
    :param replace: Whether you want to replace existing destination files
        or not.
    """


    template_fields = ('remote_filepath', 'destination_gcs_bucket', 'destination_gcs_path')
    def __init__(self,
                 ssh_hook=None,
                 ssh_conn_id=None,
                 remote_host=None,
                 remote_filepath=None,
                 destination_gcs_bucket=None,
                 destination_gcs_path=None,
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(SFTPToGCSOperator, self).__init__(*args, **kwargs)
        self.ssh_hook = ssh_hook
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.remote_filepath = remote_filepath
        self.destination_gcs_bucket = destination_gcs_bucket
        self.destination_gcs_path = destination_gcs_path
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        gcs_hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        try:
            if self.ssh_conn_id and not self.ssh_hook:
                self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)

            if not self.ssh_hook:
                raise AirflowException('can not operate without ssh_hook or ssh_conn_id')

            if self.remote_host is not None:
                self.ssh_hook.remote_host = self.remote_host

            with self.ssh_hook.get_conn() as ssh_client:
                sftp_client = ssh_client.open_sftp()

                with NamedTemporaryFile('wb') as temp_file:
                    sftp_client.get(self.remote_filepath, temp_file.name)
                    gcs_hook.upload(self.destination_gcs_bucket, self.destination_gcs_path, temp_file.name)
        except Exception as error_object:
            raise AirflowException('Error while transferring. Error details: {1}'
                                   .format(str(error_object)))

        return None

class SFTPToGCSPlugin(AirflowPlugin):
    """
    define the plugin class
    """
    name = 'sftp_to_gcs_plugin'
    operators = [SFTPToGCSOperator]
