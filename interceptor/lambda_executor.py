import json
import time
import re
import shutil
from json import dumps, loads
from pathlib import Path
from subprocess import Popen, PIPE
from time import sleep
from traceback import format_exc
from typing import Tuple, Union
from uuid import uuid4

from docker import DockerClient
from docker.errors import APIError
from docker.models.volumes import Volume
from docker.types import Mount
from requests import get, put


from interceptor.constants import NAME_CONTAINER_MAPPING
from interceptor.containers_backend import KubernetesClient
from interceptor.logger import logger as global_logger
from interceptor.utils import build_api_url


class LambdaExecutor:

    def __init__(self, task: dict, event: Union[dict, list], galloper_url: str, token: str,
                 mode: str = 'default', logger=global_logger, **kwargs):
        self.logger = logger
        self.task = task
        if isinstance(event, list):
            self.event = event[0]
        else:
            self.event = event
        self.galloper_url = galloper_url
        self.token = token
        self.mode = mode
        self.start_time = time.time()
        self.api_version = kwargs.get('api_version', 1)
        self.api_headers = {
            'Content-Type': 'application/json',
            'Authorization': f'{kwargs.get("token_type", "bearer")} {self.token}'
        }

        self.env_vars = loads(self.task.get("env_vars", "{}"))
        if self.task['task_name'] == "control_tower" and "cc_env_vars" in self.event:
            self.env_vars.update(self.event["cc_env_vars"])

        artifact_url_part = build_api_url('artifacts', 'artifact',
                                          mode=self.mode, api_version=self.api_version)
        self.artifact_url = f'{self.galloper_url}{artifact_url_part}/' \
                            f'{self.task["project_id"]}/{self.task["zippath"]}'
        self.command = [f"{self.task['task_handler']}", dumps(self.event)]
        
        self.execution_params = None
        if self.event:
            value = self.event.get('execution_params', None)
            self.execution_params = loads(value) if value else value

    def execute_lambda(self):
        self.logger.info(f'task {self.task}')
        self.logger.info(f'event {self.event}')
        container_name = NAME_CONTAINER_MAPPING.get(self.task['runtime'])
        if not container_name:
            self.logger.error(f"Container {self.task['runtime']} is not found")
            raise Exception(f"Container {self.task['runtime']} is not found")
        try:
            cloud_settings = self.event["integration"]["clouds"]["kubernetes"]
        except (TypeError, KeyError):
            log, stats = self.execute_in_docker(container_name)
        else:
            log, stats = self.execute_in_kubernetes(container_name, cloud_settings)

        if container_name == "lambda:python3.7":
            results = re.findall(r'({.+?})', log)[-1]
        else:
            # TODO: magic of 2 enters is very flaky, Need to think on how to workaround, probably with specific logging
            results = log.split("\n\n")[1]
        task_result_id = self.task["task_result_id"]
        try:
            task_status = "Done" if 200 <= int(json.loads(results).get('statusCode')) <= 299 else "Failed"
        except:
            task_status = "Failed"
        data = {
            'results': results,
            'log': log,
            'task_duration': time.time() - self.start_time,
            'task_status': task_status,
            'task_stats': stats
        }
        self.logger.info(f'Task body {data}')
        results_url = build_api_url('tasks', 'results', mode=self.mode, api_version=self.api_version)
        res = put(
            f'{self.galloper_url}{results_url}/{self.task["project_id"]}?task_result_id={task_result_id}',
            headers=self.api_headers, data=dumps(data)
        )
        self.logger.info(f'Created task_results: {res.status_code, res.text}')

        # this was here to chain task executions, but is temporary disabled
        # if self.task.get("callback"):
        #     for each in self.event:
        #         each['result'] = results
        #     endpoint = f"/api/v1/task/{self.task['project_id']}/{self.task['callback']}?exec=True"
        #     self.task = get(f"{self.galloper_url}/{endpoint}", headers=self.api_headers).json()
        #     self.execute_lambda()
        self.logger.info('Done.')

    def execute_in_kubernetes(self, container_name: str, cloud_settings: dict):
        kubernetes_settings = {
            "host": cloud_settings["hostname"],
            "token": cloud_settings["k8s_token"],
            "namespace": cloud_settings["namespace"],
            "jobs_count": 1,
            "logger": self.logger,
            "secure_connection": cloud_settings["secure_connection"],
            "mode": self.mode
        }
        client = KubernetesClient(**kubernetes_settings)
        job = client.run_lambda(container_name, self.token, self.env_vars, self.artifact_url,
                                self.command)

        while not job.is_finished():
            sleep(5)
        logs = []
        job.log_status(logs)
        # TODO: grab stats from kubernetes
        return "".join(logs), {}

    def execute_in_docker(self, container_name: str) -> Tuple[str, dict]:
        ATTEMPTS_TO_REMOVE_VOL = 3

        lambda_id = str(uuid4())
        client = DockerClient.from_env()

        self.download_artifact(lambda_id)
        volume = self.create_volume(client, lambda_id)
        mounts = [Mount(type='volume', source=volume.name, target='/var/task')]

        try:
            code_path = self.execution_params.get('code_path')
            if code_path:
                mounts.append(Mount(type='bind', source=code_path, target='/code'))
        except AttributeError:
            ...

        container = client.containers.run(
            f'getcarrier/{container_name}',
            command=self.command,
            mounts=mounts,
            stderr=True,
            remove=True,
            environment=self.env_vars,
            detach=True
        )
        try:
            self.logger.info(f'container obj {container}')
            container_stats = container.stats(decode=False, stream=False)
            container_logs = container.logs(stream=True, follow=True)
        except Exception as e:
            self.logger.info(f'logs are not available {e}')
            self.logger.info(f'exc {format_exc()}')
            return "\n\n{logs are not available}", {}

        logs = []
        for i in container_logs:
            line = i.decode('utf-8', errors='ignore')
            self.logger.info(f'{container_name} - {line}')
            logs.append(line)

        self.logger.info(f'Log stream ended for {container_name}')

        logs = ''.join(logs)
        match = re.search(r'memory used: (\d+ \w+).*?', logs, re.I)
        try:
            container_stats['memory_usage'] = match.group(1)
        except AttributeError:
            ...

        for _ in range(ATTEMPTS_TO_REMOVE_VOL):
            sleep(1)
            try:
                volume.remove(force=True)
                self.logger.info(f'Volume removed {volume}')
                shutil.rmtree(volume._centry_path, ignore_errors=True)
                self.logger.info(f'Volume path cleared {volume._centry_path}')
                break
            except APIError:
                self.logger.info(f'Failed to remove volume. Sleeping for 1. Attempt {i + 1}/{ATTEMPTS_TO_REMOVE_VOL}')

        else:
            self.logger.warning(f'Failed to remove docker volume after {ATTEMPTS_TO_REMOVE_VOL} attempts')

        return logs, container_stats

    def download_artifact(self, lambda_id: str) -> None:
        download_path = Path('/', 'tmp', lambda_id)
        download_path.mkdir()
        headers = {'Authorization': f'bearer {self.token}'}
        r = get(self.artifact_url, allow_redirects=True, headers=headers)
        with open(download_path.joinpath(lambda_id), 'wb') as file_data:
            file_data.write(r.content)

    @staticmethod
    def create_volume(client: DockerClient, lambda_id: str) -> Volume:
        volume = client.volumes.create(lambda_id)
        volume._centry_path = Path('/', 'tmp', volume.name)
        # LambdaExecutor.unzip_python(volume)
        LambdaExecutor.unzip_docker(volume)
        return volume

    @staticmethod
    def unzip_docker(volume: Volume) -> None:
        UNZIP_DOCKERFILE = """
FROM kubeless/unzip:latest
ADD {localfile} /tmp/{docker_path}
ENTRYPOINT ["unzip", "/tmp/{docker_path}", "-d", "/tmp/unzipped"]
        """

        UNZIP_DOCKER_COMPOSE = """
version: '3'
services:
  unzip:
    build: {path}
    volumes:
      - {volume}:/tmp/unzipped
    labels:
      - 'traefik.enable=false'
    container_name: unzip-{task_id}
volumes:
  {volume}:
    external: true
        """
        volume_path = volume._centry_path
        with open(volume_path.joinpath('Dockerfile'), 'w') as f:
            f.write(UNZIP_DOCKERFILE.format(
                localfile=volume.name,
                docker_path=f'{volume.name}.zip'
            ))
        with open(volume_path.joinpath('docker-compose.yaml'), 'w') as f:
            f.write(UNZIP_DOCKER_COMPOSE.format(
                path=volume_path,
                volume=volume.name,
                task_id=volume.name
            ))
        cmd = ['docker', 'compose', 'up']
        popen = Popen(cmd, stdout=PIPE, stderr=PIPE, universal_newlines=True,
                      cwd=volume_path)
        popen.communicate()
        cmd = ['docker', 'compose', 'down', '--rmi', 'all']
        popen = Popen(cmd, stdout=PIPE, stderr=PIPE, universal_newlines=True,
                      cwd=volume_path)
        popen.communicate()

    @staticmethod
    def unzip_local(volume: Volume) -> None:
        volume_path = volume._centry_path
        cmd = ['unzip', volume.name]
        popen = Popen(cmd, stdout=PIPE, stderr=PIPE, universal_newlines=True,
                      cwd=volume_path)
        popen.communicate()

    @staticmethod
    def unzip_python(volume: Volume) -> None:
        from zipfile import ZipFile
        with ZipFile(volume._centry_path.joinpath(volume.name), 'r') as zip_ref:
            zip_ref.extractall(volume._centry_path)
