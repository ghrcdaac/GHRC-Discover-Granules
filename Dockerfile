FROM amazon/aws-lambda-python:3.9

RUN pwd
RUN yum -y update
RUN yum -y install zip

RUN mkdir ./package
COPY requirements.txt .
RUN pip install --target ./package -r requirements.txt

RUN mkdir ./package/task
COPY task/ ./package/task

WORKDIR ./package
RUN zip -r package.zip .
