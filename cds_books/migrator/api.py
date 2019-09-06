# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2019 CERN.
#
# cds-books is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS-Books migrator API."""

import uuid

from invenio_app_ils.search.api import DocumentSearch, SeriesSearch
from invenio_app_ils.records.api import Document
from invenio_app_ils.pidstore.providers import DocumentIdProvider
from invenio_db import db


def get_multipart_by_legacy_recid(recid):
    search = SeriesSearch()
    return search.filter('term', legacy_recid=recid).execute()


def create_multipart_volumes(pid, multipart_legacy_recid, migration_volumes):
    """Create multipart volume documents."""
    volumes = {}
    # Combine all volume data by volume number
    for obj in migration_volumes:
        volume_number = obj['volume']
        if volume_number not in volumes:
            volumes[volume_number] = {}
        volume = volumes[volume_number]
        for key in obj:
            if key != 'volume':
                volume[key] = obj[key]

    volume_numbers = iter(sorted(volumes.keys()))

    # Re-use the current record for the first volume
    first_volume = next(volume_numbers)
    first = Document.get_record_by_pid(pid)
    if 'title' in volumes[first_volume]:
        first['title']['title'] = volumes[first_volume]['title']
    first.commit()

    # Create new records for the rest
    records = [first]
    for number in volume_numbers:
        temp = first.copy()
        temp['title']['title'] = volumes[number]['title']
        record_uuid = uuid.uuid4()
        provider = DocumentIdProvider.create(
            object_type='rec',
            object_uuid=record_uuid,
        )
        temp['pid'] = provider.pid.pid_value
        record = Document.create(temp, record_uuid)
        record.commit()
        records.append(record)
    return records


def link_and_create_multipart_volumes(dry_run):
    """Link and create multipart volume records."""
    search = DocumentSearch().filter('term', _migration__is_multipart=True)

    for hit in search.scan():
        multipart = get_multipart_by_legacy_recid(hit.legacy_recid)
        if multipart:
            print('yay')
            documents = create_multipart_volumes(
                hit.pid,
                hit.legacy_recid,
                hit._migration.volumes
            )
        else:
            print('Failed to fetch multipart with recid {}'.format(hit.legacy_recid))

    db.session.commit()
