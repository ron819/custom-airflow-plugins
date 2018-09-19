# -*- coding: utf-8 -*-
#
# The MIT License (MIT)
#
# Copyright (c) John Lewis & Partners
#
# All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

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
