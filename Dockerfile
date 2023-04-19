FROM amazon/aws-lambda-python:3.8

RUN pwd
RUN yum -y update
RUN yum -y install zip
#RUN yum install -y git
#RUN yum -y install gcc
#RUN yum -y groupinstall "Development Tools"
#
#RUN git clone https://github.com/psycopg/psycopg2.git
#RUN git clone https://github.com/postgres/postgres.git
#
#WORKDIR postgres
#RUN ./configure --prefix "/var/task/postgres" --without-readline --without-zlib
#RUN make
#RUN make install
#
#WORKDIR ../psycopg2
#RUN sed -i "s|pg_config=|pg_config=/var/task/postgres/bin/pg_config |g" setup.cfg
#RUN sed -i "s|static_libpq=0|static_libpq=1 |g" setup.cfg
#RUN python3 setup.py build

#WORKDIR ../
#COPY task/ ./task
#COPY requirements.txt .
#COPY createPackage.py ./
#RUN python createPackage.py


RUN mkdir ./package
COPY requirements.txt .
RUN pip install --target ./package -r requirements.txt

#RUN pip install -r requirements.txt

RUN mkdir ./package/task
COPY task/ ./package/task

#WORKDIR ./package/task
#RUN python -m pytest

WORKDIR ./package
RUN zip -r package.zip .


#RUN wget https://ftp.postgresql.org/pub/source/v10.21/postgresql-10.21.tar.gz
#RUN tar -xvzf postgresql-10.21.tar.gz

#RUN git clone https://github.com/psycopg/psycopg2.git
#
#RUN wget https://ftp.postgresql.org/pub/source/v10.21/postgresql-10.21.tar.gz
#RUN tar -xvzf postgresql-10.21.tar.gz
#
#WORKDIR postgresql-10.21
#RUN ./configure --prefix /postgresql --without-readline --without-zlib
#RUN make
#RUN make install
#
#WORKDIR ../psycopg2
#RUN sed -i "s|pg_config=|pg_config=/postgresql-10.21/src/bin/pg_config/pg_config |g" setup.cfg
#RUN sed -i "s|static_libpq=0|static_libpq=1 |g" setup.cfg
#RUN python3 setup.py build
