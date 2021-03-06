#!/usr/bin/env bash
# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 CERN.
#
# CDS Books is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

# Ignoring false positive 36759 (reporting invenio-admin v1.0.1). This can be
# removed when https://github.com/pyupio/safety-db/pull/2274 is merged and
# released.

# Ignoring 36810 (insecure numpy version). This can be removed when Travis
# updates numpy (https://travis-ci.community/t/issue-pipenv-check-fails-due-to-numpy/2120/3).
pipenv check --ignore 36759 --ignore 36810 && \
pipenv run pydocstyle cds_books tests docs && \
pipenv run isort -rc -c -df && \
pipenv run check-manifest --ignore ".travis-*,docs/_build*" && \
pipenv run sphinx-build -qnNW docs docs/_build/html && \
pipenv run test
