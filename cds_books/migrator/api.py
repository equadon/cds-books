# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2019 CERN.
#
# cds-books is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS-Books migrator API."""

from invenio_app_ils.search.api import DocumentSearch
from invenio_app_ils.records.api import Document


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
    exit()

    # Create new records for the rest


def link_and_create_multipart_volumes(dry_run):
    """Link and create multipart volume records."""
    search = DocumentSearch().filter('term', _migration__record_type='multipart')

    for hit in search.scan():
        import ipdb; ipdb.set_trace()
        create_multipart_volumes(
            hit.pid,
            hit.legacy_recid,
            hit._migration.volumes
        )
