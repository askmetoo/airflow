#  Licensed to the Apache Software Foundation (ASF) under one   *
#  or more contributor license agreements.  See the NOTICE file *
#  distributed with this work for additional information        *
#  regarding copyright ownership.  The ASF licenses this file   *
#  to you under the Apache License, Version 2.0 (the            *
#  "License"); you may not use this file except in compliance   *
#  with the License.  You may obtain a copy of the License at   *
#                                                               *
#    http://www.apache.org/licenses/LICENSE-2.0                 *
#                                                               *
#  Unless required by applicable law or agreed to in writing,   *
#  software distributed under the License is distributed on an  *
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
#  KIND, either express or implied.  See the License for the    *
#  specific language governing permissions and limitations      *
#  under the License.                                           *

FROM ubuntu:16.04

# install deps
RUN apt-get update -y && apt-get install -y \
        wget \
        python-dev \
        python-pip \
        libczmq-dev \
        libcurlpp-dev \
        ntp \
        curl \
        libssl-dev \
        libmysqlclient-dev \
        git \
        inetutils-telnet \
        bind9utils

RUN pip install -U setuptools && \
    pip install -U pip==9.0.1

RUN pip install kubernetes==5.0.0 && \
    pip install cryptography==2.2.1 && \
    pip install boto3==1.6.16 && \
    pip install sendgrid==5.3.0 && \
    pip install mysqlclient==1.3.12 && \
    pip install analytics-python==1.2.9 && \
    pip install hive-thrift-py==0.0.1 && \
    pip install PyHive==0.5.1 && \
    pip install impyla==0.14.1 && \
    pip install unicodecsv==0.14.1 && \
    pip install psycopg2==2.7.3.1  # I had issues with older versions

# install airflow
COPY airflow.tar.gz /tmp/airflow.tar.gz
RUN pip install /tmp/airflow.tar.gz

COPY bootstrap.sh /bootstrap.sh
RUN chmod +x /bootstrap.sh

ENTRYPOINT ["/bootstrap.sh"]
