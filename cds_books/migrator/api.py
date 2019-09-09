# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2019 CERN.
#
# cds-books is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS-Books migrator API."""

import click
import json
import uuid

from elasticsearch_dsl import Q
from flask import current_app

from invenio_app_ils.search.api import DocumentSearch, SeriesSearch
from invenio_app_ils.records.api import Document, Series, Keyword
from invenio_app_ils.records_relations.api import RecordRelationsParentChild
from invenio_app_ils.search.api import DocumentSearch
from invenio_app_ils.pidstore.providers import DocumentIdProvider, \
    SeriesIdProvider
from invenio_base.app import create_cli
from invenio_db import db
from invenio_indexer.api import RecordIndexer
from invenio_migrator.cli import _loadrecord, dumps
from invenio_pidstore.errors import PIDAlreadyExists
from invenio_pidstore.models import PersistentIdentifier
from invenio_records import Record
from invenio_records.models import RecordMetadata

from cds_books.migrator.errors import LossyConversion
from cds_books.migrator.records import CDSParentRecordDumpLoader


def reindex_pidtype(pid_type):
    """Reindex records with the specified pid_type."""
    click.echo('Indexing pid type "{}"...'.format(pid_type))
    cli = create_cli()
    runner = current_app.test_cli_runner()
    runner.invoke(
        cli,
        'index reindex --pid-type {} --yes-i-know'.format(pid_type),
        catch_exceptions=True
    )
    runner.invoke(cli, 'index run', catch_exceptions=False)
    click.echo('Indexing completed!')


def bulk_index_records(records):
    """Bulk index a list of records."""
    indexer = RecordIndexer()

    click.echo('Bulk indexing {} records...'.format(len(records)))
    indexer.bulk_index([str(r.id) for r in records])
    indexer.process_bulk_queue()
    click.echo('Indexing completed!')


def model_provider_by_rectype(rectype):
    """Return the correct model and PID provider based on the rectype."""
    if rectype in ('serial', 'multipart'):
        return Series, SeriesIdProvider
    elif rectype == 'document':
        return Document, DocumentIdProvider


def import_parents_from_file(dump_file, rectype, include):
    """Load parent records from file."""
    model, provider = model_provider_by_rectype(rectype)
    include_keys = None if include is None else include.split(',')
    with click.progressbar(json.load(dump_file).items()) as bar:
        records = []
        for key, parent in bar:
            if include_keys is None or key in include_keys:
                record = import_parent_record(parent, model, provider)
                click.echo('Loaded {} with PID "{}"...'.format(
                    rectype,
                    record["pid"]
                ))
                records.append(record)
    # Index all new parent records
    bulk_index_records(records)


def import_parent_record(dump, model, pid_provider):
    try:
        record = CDSParentRecordDumpLoader.create(dump, model, pid_provider)
        db.session.commit()
        return record
    except Exception:
        db.session.rollback()
        raise


def import_records_from_dump(sources, source_type, eager, include):
    """Load records."""
    include = include if include is None else include.split(',')
    for idx, source in enumerate(sources, 1):
        click.echo('Loading dump {0} of {1} ({2})'.format(
            idx, len(sources), source.name))
        data = json.load(source)
        with click.progressbar(data) as records:
            for item in records:
                if include is None or str(item['recid']) in include:
                    try:
                        _loadrecord(item, source_type, eager=eager)
                        click.echo('Loaded record with legacy recid: {}'.format(
                            item['recid']))
                    except PIDAlreadyExists:
                        current_app.logger.warning(
                            "migration: report number associated with multiple"
                            "recid. See {0}".format(item['recid']))
                    except LossyConversion:
                        pass
    # We don't get the record back from _loadrecord so re-index all documents
    reindex_pidtype('docid')



def get_multipart_by_legacy_recid(recid):
    search = SeriesSearch()
    result = search.filter('term', legacy_recid=recid).execute()
    if not result:
        raise Exception('no multipart with legacy recid {}'.format(recid))
    if result.hits.total == 1:
        return Series.get_record_by_pid(result.hits[0].pid)
    else:
        raise Exception('multiple hits for multipart legacy recid {}'.format(recid))


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
        first['volume'] = first_volume
    first.commit()
    yield first

    # Create new records for the rest
    for number in volume_numbers:
        temp = first.copy()
        temp['title']['title'] = volumes[number]['title']
        temp['volume'] = number
        record_uuid = uuid.uuid4()
        provider = DocumentIdProvider.create(
            object_type='rec',
            object_uuid=record_uuid,
        )
        temp['pid'] = provider.pid.pid_value
        record = Document.create(temp, record_uuid)
        record.commit()
        yield record


def create_parent_child_relation(parent, child, relation_type, volume):
    """Create parent child relations."""
    rr = RecordRelationsParentChild()
    rr.add(
        parent=parent,
        child=child,
        relation_type=relation_type,
        volume=volume
    )


def link_and_create_multipart_volumes(dry_run):
    """Link and create multipart volume records."""
    search = DocumentSearch().filter('term', _migration__is_multipart=True)

    for hit in search.scan():
        multipart = get_multipart_by_legacy_recid(hit.legacy_recid)
        documents = create_multipart_volumes(
            hit.pid,
            hit.legacy_recid,
            hit._migration.volumes
        )
        for document in documents:
            create_parent_child_relation(
                multipart,
                document,
                current_app.config['MULTIPART_MONOGRAPH_RELATION'],
                document['volume']
            )

    if not dry_run:
        db.session.commit()


def get_serial_by_title(title):
    """Get serial record by title."""
    search = SeriesSearch().query(
        'bool',
        filter=[
            Q('term', mode_of_issuance='SERIAL'),
            Q('term', title__title=title),
        ]
    )
    results = search.execute()
    if results.hits.total == 1:
        return Series.get_record_by_pid(results.hits[0].pid)
    raise Exception(
        'Found 0 or more than 1 serial with title "{}"'.format(title))


def link_and_create_serials(dry_run):
    """Link and create serial records."""
    search = DocumentSearch().filter('term', _migration__has_serial=True)

    for hit in search.scan():
        document = Document.get_record_by_pid(hit.pid)
        for obj in hit._migration.serials:
            serial = get_serial_by_title(obj['title'])
            create_parent_child_relation(
                serial,
                document,
                current_app.config['SERIAL_RELATION'],
                obj['volume']
            )

    if not dry_run:
        db.session.commit()
