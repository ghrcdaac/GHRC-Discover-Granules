FROM amazon/aws-lambda-python:3.10

RUN yum -y update
RUN yum -y install zip

ENV package_dir=/package
RUN mkdir ${package_dir}
WORKDIR ${package_dir}
COPY requirements.txt .
RUN pip install --target . -r requirements.txt

ENV package_name=ghrc_discover_granules_lambda.zip
RUN zip -r ${package_name} .

ENV task_dir=${package_dir}/task
RUN mkdir ${task_dir}
COPY task/ ${task_dir}
RUN zip -ru ${package_name} .
RUN mv ${package_name} /

ENTRYPOINT ["python", "-m", "task.ecs_service"]
#CMD ["-m task.lambda_function ${event}"]
#CMD ["-sh", "-c", "python -m task.lambda_function ${event}"]
