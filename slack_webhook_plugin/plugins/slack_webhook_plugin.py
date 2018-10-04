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
This contains one plugin which sends a message to slack
"""
import json
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin

class SlackWebhookOperator(BaseOperator):
    """
    Sends a message to slack using slack incoming webhook.
    See https://api.slack.com/incoming-webhooks for details on how to enable and set one up in slack
    Once set up, you will get a webhook URL to use
        it will be something like https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX
    In airflow you should set a http endpoint with a host of https://hooks.slack.com/ which is passed into this operator
    You should also pass in slack_endpoint to this operator which is the path on the slack webhook url
        eg /services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX

    :param http_conn_id: connection to slack.
    :type http_conn_id: string
    :param message: message to send
    :type message: string
    :param channel: channel to send message to
    :type channel: string
    :param username: username to use when sending message
    :type username: string
    :param slack_endpoint: endpoint to use in incoming webhook.
    :type slack_endpoint: string
    """
    def __init__(self,
                 http_conn_id='slack_endpoint',
                 message='',
                 channel=None,
                 username='airflow',
                 slack_endpoint=None,
                 *args,
                 **kwargs):
        super(SlackWebhookOperator, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.message = message
        self.channel = channel
        self.username = username
        self.slack_endpoint = slack_endpoint
        self.hook = None

    def execute(self, context):
        http = HttpHook(method='POST', http_conn_id=self.http_conn_id)
        data = {
            'text': self.message,
            'username': self.username,
            'channel': self.channel
            }
        headers = {'Content-Type': 'application/json'}
        self.log.info('Calling HTTP method')

        response = http.run(self.slack_endpoint,
                            data=json.dumps(data),
                            headers=headers)
        return response.text


class SlackWebhookPlugin(AirflowPlugin):
    """
    define the plugin class
    """
    name = 'slack_webhook_plugin'
    operators = [SlackWebhookOperator]
