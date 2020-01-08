# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 CERN.
#
# CDS Books is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.
#
# Dockerfile that builds a fully functional image of your app.
#
# Note: It is important to keep the commands in this file in sync with your
# boostrap script located in ./scripts/bootstrap.
#
# In order to increase the build speed, we are extending this image from a base
# image (built with Dockerfile.base) which only includes your Python
# dependencies.
FROM inveniosoftware/centos7-python:3.6

# uWSGI configuration to be changed
ARG UWSGI_WSGI_MODULE=invenio_app.wsgi:application
ENV UWSGI_WSGI_MODULE ${UWSGI_WSGI_MODULE:-invenio_app.wsgi:application}
ARG UWSGI_PORT=5000
ENV UWSGI_PORT ${UWSGI_PORT:-5000}
ARG UWSGI_PROCESSES=2
ENV UWSGI_PROCESSES ${UWSGI_PROCESSES:-2}
ARG UWSGI_THREADS=2
ENV UWSGI_THREADS ${UWSGI_THREADS:-2}

# We invalidate cache always because there is no easy way for now to detect
# if something in the whole git repo changed. For docker git clone <url> <dir>
# is always the same so it caches it.
ARG CACHE_DATE=not_a_date

ENV BACKEND_WORKING_DIR=${WORKING_DIR}/src/backend

RUN mkdir -p ${BACKEND_WORKING_DIR}
COPY ./ ${BACKEND_WORKING_DIR}
WORKDIR ${BACKEND_WORKING_DIR}

ENV INVENIO_STATIC_URL_PATH='/invenio-assets'
ENV INVENIO_STATIC_FOLDER=${INVENIO_INSTANCE_PATH}/invenio-assets

# needed system dependency for python-ldap
RUN yum install -y openldap-devel

# Installs all packages specified in Pipfile.lock
RUN pipenv install --deploy --system --ignore-pipfile && \
    pipenv run pip install . && \
    pipenv run invenio collect -v && \
    pipenv run invenio webpack create && \
    # --unsafe needed because we are running as root
    pipenv run invenio webpack install --unsafe && \
    pipenv run invenio webpack build

CMD [ "bash", "-c", "uwsgi --module ${UWSGI_WSGI_MODULE} --socket 0.0.0.0:${UWSGI_PORT} --master --processes ${UWSGI_PROCESSES} --threads ${UWSGI_THREADS} --stats /tmp/stats.socket"]
