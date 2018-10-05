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
This contains a plugin that can be used to set the description on a big query table
"""

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

# pylint: disable=W0223
class CustomBigQueryHook(BigQueryHook):
    """
    This defines a subclass of the BigQueryHook class
    It has an extra function used to set the table description
    """

    def set_table_description(self, dataset_id, table_id, description, project_id=None):
        """
        Sets the description for the given table

        :param project_id: The Google cloud project in which to look for the
            table. The connection supplied to the hook must provide access to
            the specified project.
        :type project_id: string
        :param dataset_id: The name of the dataset in which to look for the
            table.
        :type dataset_id: string
        :param table_id: The name of the table to set the description for.
        :type table_id: string
        :param description: The description to set
        :type description: string
        """
        service = self.get_service()
        project_id = project_id if project_id is not None else self._get_field('project')
        table = service.tables().get(
            projectId=project_id, datasetId=dataset_id,
            tableId=table_id).execute()
        self.log.info('table: %s', table)
        table['description'] = description
        service.tables().patch(
            projectId=project_id,
            datasetId=dataset_id,
            tableId=table_id,
            body=table).execute()

class SetBQTableDescriptionOperator(BaseOperator):
    """
    This is the operator that is called to set the desription

    :param project_id: The Google cloud project in which to look for the
        table. The connection supplied must provide access to
        the specified project.
    :type project_id: string
    :param dataset_id: The name of the dataset in which to look for the
        table.
    :type dataset_id: string
    :param table_id: The name of the table to set the description for.
    :type table_id: string
    :param description: The description to set
    :type description: string
    :param bigquery_conn_id: The connection ID to use when
        connecting to BigQuery.
    :type google_cloud_storage_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide delegation enabled.
    :type delegate_to: string
    """
    @apply_defaults
    def __init__(self,
                 project_id=None,
                 dataset_id=None,
                 table_id=None,
                 description=None,
                 bigquery_conn_id='bigquery_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(SetBQTableDescriptionOperator, self).__init__(*args, **kwargs)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.description = description
        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        hook = CustomBigQueryHook(
            bigquery_conn_id=self.bigquery_conn_id,
            delegate_to=self.delegate_to)
        hook.set_table_description(project_id=self.project_id,
                                   dataset_id=self.dataset_id,
                                   table_id=self.table_id,
                                   description=self.description)


class BigQueryCustomPlugin(AirflowPlugin):
    """
    define the plugin class
    """
    name = 'BigQueryCustomPlugin'
    operators = [SetBQTableDescriptionOperator]
