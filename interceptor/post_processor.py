import json
from os import path, environ

import requests

from interceptor.lambda_executor import LambdaExecutor
from interceptor.logger import logger as global_logger


class PostProcessor:

    def __init__(self, galloper_url, project_id, galloper_web_hook, report_id, bucket, prefix,
            logger=global_logger, token=None, integration=[]
    ):
        self.logger = logger
        self.galloper_url = galloper_url
        self.project_id = project_id
        self.galloper_web_hook = galloper_web_hook
        self.bucket = bucket
        self.prefix = prefix
        self.config_file = '{}'
        self.token = token
        self.integration = integration
        self.report_id = report_id

    def update_test_status(self, status, percentage, description):
        data = {"test_status": {"status": status, "percentage": percentage,
                                "description": description}}
        headers = {'content-type': 'application/json', 'Authorization': f'bearer {self.token}'}
        url = f'{self.galloper_url}/api/v1/backend_performance/report_status/' \
              f'{self.project_id}/{self.report_id}'
        response = requests.put(url, json=data, headers=headers)
        try:
            self.logger.info(response.json()["message"])
        except:
            self.logger.info(response.text)

    def results_post_processing(self):
        if self.galloper_web_hook:
            if path.exists('/tmp/config.yaml'):
                with open("/tmp/config.yaml", "r") as f:
                    self.config_file = f.read()
            else:
                self.config_file = environ.get('CONFIG_FILE', '{}')

            event = {'galloper_url': self.galloper_url, 'project_id': self.project_id,
                     'config_file': json.dumps(self.config_file),
                     'bucket': self.bucket, 'prefix': self.prefix, 'token': self.token,
                     'integration': self.integration, "report_id": self.report_id}
            endpoint = f"api/v1/tasks/task/{self.project_id}/" \
                       f"{self.galloper_web_hook.replace(self.galloper_url + '/task/', '')}?exec=True"
            headers = {'Authorization': f'bearer {self.token}',
                       'content-type': 'application/json'}
            task = requests.get(f"{self.galloper_url}/{endpoint}", headers=headers).json()
            try:
                LambdaExecutor(task, event, self.galloper_url, self.token,
                               self.logger).execute_lambda()
            except Exception as exc:
                self.update_test_status("Error", 100, f"Failed to start postprocessing")
                raise exc
