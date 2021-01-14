import boto3
import os
from time import sleep, time
from logs.log import logger
from kubernetes import client, config
import string
import random

class SQSPoller:

    options = None
    sqs_client = None
    extensions_v1_beta1 = None
    last_message_count = None

    def __init__(self, options):
        self.options = options
        print(options)
        if self.options.local:
            self.options.image_pull_policy = 'Never'
        else:
            self.options.image_pull_policy = 'Always'
        self.sqs_client = boto3.client('sqs', aws_secret_access_key=options.aws_secret_access_key, aws_access_key_id=options.aws_access_key_id, region_name=options.aws_region)
        config.load_incluster_config()
        self.extensions_v1_beta1 = client.AppsV1Api()
        self.batch_v1_api = client.BatchV1Api()
        self.last_scale_up_time = time()
        self.last_scale_down_time = time()

    def message_count(self):
        response = self.sqs_client.get_queue_attributes(
            QueueUrl=self.options.sqs_queue_url,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        return int(response['Attributes']['ApproximateNumberOfMessages'])


    def poll(self):
        message_count = self.message_count()
        t = time()
        if  message_count >= self.options.scale_up_messages/self.options.job_concurrency:
            if t - self.last_scale_up_time > self.options.scale_up_cool_down:
                self.scale_up()
                self.last_scale_up_time = t
            else:
                logger.debug("Waiting for scale up cooldown")
        # if message_count <= self.options.scale_down_messages:
        #     if t - self.last_scale_down_time > self.options.scale_down_cool_down:
        #         self.scale_down()
        #         self.last_scale_down_time = t
        #     else:
        #         logger.debug("Waiting for scale down cooldown")

        # code for scale to use msg_count
        sleep(self.options.poll_period)

    def scale_up(self):
        jobs = self.jobs_count()
        if jobs < self.options.max_pods:
            logger.info("Scaling up")
            self.launch_job()
        else:
            logger.info("Max pods reached")
        # deployment = self.deployment()
        # if deployment.spec.replicas < self.options.max_pods:
        #     logger.info("Scaling up")
        #     deployment.spec.replicas += 1
        #     self.update_deployment(deployment)
        # elif deployment.spec.replicas > self.options.max_pods:
        #     self.scale_down()
        # else:
        #     logger.info("Max pods reached")

    def scale_down(self):
        pass
        # deployment = self.deployment()
        # if deployment.spec.replicas > self.options.min_pods:
        #     logger.info("Scaling Down")
        #     deployment.spec.replicas -= 1
        #     self.update_deployment(deployment)
        # elif deployment.spec.replicas < self.options.min_pods:
        #     self.scale_up()
        # else:
        #     logger.info("Min pods reached")

    def jobs_count(self):
        # Get Running Jobs:
        pretty = 'true'
        logger.debug("loading jobs from namespace: {}".format(self.options.kubernetes_namespace))
        # print(self.batch_v1_api.list_namespaced_job(self.options.kubernetes_namespace, pretty=pretty))
        jobs = self.batch_v1_api.list_namespaced_job(self.options.kubernetes_namespace, pretty=pretty)
        if jobs.items == None or len(jobs.items) == 0:
            return 0
        else:
            jobs_running = list(filter(lambda x: x.status.active is not None, jobs.items))
            jobs_count = 0
            for j in jobs_running:
                jobs_count += j.status.active
            return jobs_count

    def deployment(self):
        logger.debug("loading deployment: {} from namespace: {}".format(self.options.kubernetes_deployment, self.options.kubernetes_namespace))
        # print(self.extensions_v1_beta1.list_namespaced_deployment(self.options.kubernetes_namespace))
        deployments = self.extensions_v1_beta1.list_namespaced_deployment(self.options.kubernetes_namespace, label_selector="app={}".format(self.options.kubernetes_deployment))
        # print(deployments)
        return deployments.items[0]

    def update_deployment(self, deployment):
        # Update the deployment
        api_response = self.extensions_v1_beta1.patch_namespaced_deployment(
            name=self.options.kubernetes_deployment,
            namespace=self.options.kubernetes_namespace,
            body=deployment)
        logger.debug("Deployment updated. status='%s'" % str(api_response.status))

    def id_generator(self, size=12, chars=string.ascii_lowercase + string.digits, name_prefix=''):
        rand_id = ''.join(random.choice(chars) for _ in range(size))
        return ''.join([name_prefix, '-', rand_id])

    def create_job_body(self, container_name, container_image, env_vars={}):
        body = client.V1Job(api_version="batch/v1", kind="Job")
        body.metadata = client.V1ObjectMeta(namespace=self.options.kubernetes_namespace, name=self.id_generator(name_prefix=container_name))
        body.status = client.V1JobStatus()
        template = client.V1PodTemplate()
        template.template = client.V1PodTemplateSpec()
        env_list = []
        for env_name, env_value in env_vars.items():
            env_list.append(client.V1EnvVar(name=env_name, value=env_value))
        # container = client.V1Container(name=container_name, image=container_image, env=env_list, command=["perl",  "-Mbignum=bpi", "-wle", "print bpi(2000)"])
        config_map_ref = client.V1ConfigMapEnvSource(name='aws-config')
        config_source = [client.V1EnvFromSource(config_map_ref)]
        container = client.V1Container(name=container_name, image=container_image, env_from=config_source, image_pull_policy=self.options.image_pull_policy)
        template.template.spec = client.V1PodSpec(containers=[container], restart_policy='Never')
        body.spec = client.V1JobSpec(completions=self.options.job_concurrency, parallelism=self.options.job_concurrency, ttl_seconds_after_finished=0, template=template.template)
        # print(body)
        return body

    def launch_job(self):
        name_prefix = 'pi'
        body = self.create_job_body(container_name=self.options.container_name, container_image=self.options.container_image, env_vars={"VAR": "TESTING"})
        api_response = self.batch_v1_api.create_namespaced_job(self.options.kubernetes_namespace, body=body, pretty=True)
        # print(api_response)

    def run(self):
        options = self.options
        print('Starting to poll')
        logger.debug("Starting poll for {} every {}s".format(options.sqs_queue_url, options.poll_period))
        while True:
            self.poll()

def run(options):
    """
    poll_period is set as as part of k8s deployment env variable
    sqs_queue_url is set as as part of k8s deployment env variable
    """
    SQSPoller(options).run()
