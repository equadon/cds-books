# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 CERN.
#
# CDS Books is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Pytest fixtures and plugins for the API application."""

from __future__ import absolute_import, print_function

import json
import os

import pytest
from invenio_accounts.models import User
from invenio_app.factory import create_api
from invenio_app_ils.records.api import Document, InternalLocation, Item, \
    Location
from invenio_circulation.api import Loan
from invenio_circulation.pidstore.pids import CIRCULATION_LOAN_PID_TYPE
from invenio_db import db
from invenio_indexer.api import RecordIndexer
from invenio_oauthclient.models import RemoteAccount, UserIdentity
from invenio_pidstore.models import PersistentIdentifier, PIDStatus
from invenio_search import current_search
from invenio_userprofiles.models import UserProfile

from invenio_app_ils.pidstore.pids import (  # isort:skip
    DOCUMENT_PID_TYPE,
    EITEM_PID_TYPE,
    ITEM_PID_TYPE,
    LOCATION_PID_TYPE,
    INTERNAL_LOCATION_PID_TYPE,
)


def load_json_from_datadir(filename):
    """Load JSON from dir."""
    _data_dir = os.path.join(os.path.dirname(__file__), "data")
    with open(os.path.join(_data_dir, filename), "r") as fp:
        return json.load(fp)


def mint_record_pid(pid_type, pid_field, record):
    """Mint the given PID for the given record."""
    PersistentIdentifier.create(
        pid_type=pid_type,
        pid_value=record[pid_field],
        object_type="rec",
        object_uuid=record.id,
        status=PIDStatus.REGISTERED,
    )
    db.session.commit()


@pytest.fixture(scope='module')
def app_config(app_config):
    """Get app config."""
    app_config["APP_ALLOWED_HOSTS"] = ["localhost"]
    return app_config


@pytest.fixture(scope='module')
def create_app():
    """Create test app."""
    return create_api


@pytest.fixture()
def testdata(app, db, es_clear, system_user):
    """Create, index and return test data."""
    indexer = RecordIndexer()

    locations = load_json_from_datadir("locations.json")
    for location in locations:
        record = Location.create(location)
        mint_record_pid(LOCATION_PID_TYPE, "pid", record)
        record.commit()
        db.session.commit()
        indexer.index(record)

    internal_locations = load_json_from_datadir("internal_locations.json")
    for internal_location in internal_locations:
        record = InternalLocation.create(internal_location)
        mint_record_pid(
            INTERNAL_LOCATION_PID_TYPE, "pid", record
        )
        record.commit()
        db.session.commit()
        indexer.index(record)

    documents = load_json_from_datadir("documents.json")
    for doc in documents:
        record = Document.create(doc)
        mint_record_pid(DOCUMENT_PID_TYPE, "pid", record)
        record.commit()
        db.session.commit()
        indexer.index(record)

    items = load_json_from_datadir("items.json")
    for item in items:
        record = Item.create(item)
        mint_record_pid(ITEM_PID_TYPE, "pid", record)
        record.commit()
        db.session.commit()
        indexer.index(record)

    loans = load_json_from_datadir("loans.json")
    for loan in loans:
        record = Loan.create(loan)
        mint_record_pid(CIRCULATION_LOAN_PID_TYPE, "pid", record)
        record.commit()
        db.session.commit()
        indexer.index(record)

    # flush all indices after indexing, otherwise ES won't be ready for tests
    current_search.flush_and_refresh(index='*')
    return {
        "locations": locations,
        "documents": documents,
        "items": items,
        "loans": loans,
    }
