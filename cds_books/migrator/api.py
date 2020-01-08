# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2019 CERN.
#
# cds-books is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS-Books migrator API."""

import json
import uuid
from contextlib import contextmanager

import click
from elasticsearch_dsl import Q
from flask import current_app
from invenio_app_ils.pidstore.providers import DocumentIdProvider, \
    SeriesIdProvider
from invenio_app_ils.records.api import Document, Series
from invenio_app_ils.records_relations.api import RecordRelationsParentChild
from invenio_app_ils.search.api import DocumentSearch, SeriesSearch
from invenio_base.app import create_cli
from invenio_db import db
from invenio_indexer.api import RecordIndexer
from invenio_migrator.cli import _loadrecord, dumps
from invenio_pidstore.errors import PIDAlreadyExists
from invenio_pidstore.models import PersistentIdentifier
from invenio_records import Record
from invenio_records.models import RecordMetadata

from cds_books.migrator.errors import DocumentMigrationError, \
    LossyConversion, MultipartMigrationError, SerialMigrationError
from cds_books.migrator.records import CDSParentRecordDumpLoader


@contextmanager
def commit():
    """Commit transaction or rollback in case of an exception."""
    try:
        yield
        db.session.commit()
    except:
        print('Rolling back changes...')
        db.session.rollback()
        raise


def reindex_pidtype(pid_type):
    """Reindex records with the specified pid_type."""
    click.echo('Indexing pid type "{}"...'.format(pid_type))
    cli = create_cli()
    runner = current_app.test_cli_runner()
    runner.invoke(
        cli,
        'index reindex --pid-type {} --yes-i-know'.format(pid_type),
        catch_exceptions=False
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
    else:
        raise ValueError('Unknown rectype: {}'.format(rectype))


def import_parents_from_file(dump_file, rectype, include):
    """Load parent records from file."""
    model, provider = model_provider_by_rectype(rectype)
    include_keys = None if include is None else include.split(',')
    with click.progressbar(json.load(dump_file).items()) as bar:
        records = []
        for key, parent in bar:
            if include_keys is None or key in include_keys:
                has_children = parent.get('_migration', {}).get('children', [])
                has_volumes = parent.get('_migration', {}).get('volumes', [])
                if rectype == 'serial' and has_children:
                    record = import_record(parent, model, provider)
                    records.append(record)
                elif rectype == 'multipart' and has_volumes:
                    record = import_record(parent, model, provider)
                    records.append(record)
    # Index all new parent records
    bulk_index_records(records)


def import_record(dump, model, pid_provider):
    """Import record in database."""
    record = CDSParentRecordDumpLoader.create(dump, model, pid_provider)
    return record


def import_documents_from_record_file(sources, include):
    """Import documents from records file generated by CDS-Migrator-Kit."""
    include = include if include is None else include.split(',')
    records = []
    for idx, source in enumerate(sources, 1):
        click.echo('({}/{}) Migrating documents in {}...'.format(
            idx, len(sources), source.name))
        model, provider = model_provider_by_rectype('document')
        include_keys = None if include is None else include.split(',')
        with click.progressbar(json.load(source).items()) as bar:
            records = []
            for key, parent in bar:
                if include_keys is None or key in include_keys:
                    record = import_record(
                        parent,
                        model,
                        provider
                    )
                    records.append(record)
    # Index all new parent records
    bulk_index_records(records)


def import_documents_from_dump(sources, source_type, eager, include):
    """Load records."""
    include = include if include is None else include.split(',')
    for idx, source in enumerate(sources, 1):
        click.echo('({}/{}) Migrating documents in {}...'.format(
            idx, len(sources), source.name))
        data = json.load(source)
        with click.progressbar(data) as records:
            for item in records:
                if include is None or str(item['recid']) in include:
                    _loadrecord(item, source_type, eager=eager)
    # We don't get the record back from _loadrecord so re-index all documents
    reindex_pidtype('docid')


def get_multipart_by_legacy_recid(recid):
    """Search multiparts by its legacy recid."""
    search = SeriesSearch().query(
        'bool',
        filter=[
            Q('term', mode_of_issuance='MULTIPART_MONOGRAPH'),
            Q('term', legacy_recid=recid),
        ]
    )
    result = search.execute()
    if result.hits.total < 1:
        raise MultipartMigrationError(
            'no multipart found with legacy recid {}'.format(recid))
    elif result.hits.total > 1:
        raise MultipartMigrationError(
            'found more than one multipart with recid {}'.format(recid))
    else:
        return Series.get_record_by_pid(result.hits[0].pid)


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
                if key in volume:
                    raise KeyError(
                        'Duplicate key "{}" for multipart {}'.format(
                            key,
                            multipart_legacy_recid
                        )
                    )
                volume[key] = obj[key]

    volume_numbers = iter(sorted(volumes.keys()))

    # Re-use the current record for the first volume
    first_volume = next(volume_numbers)
    first = Document.get_record_by_pid(pid)
    if 'title' in volumes[first_volume]:
        first['title']['title'] = volumes[first_volume]['title']
        first['volume'] = first_volume
    first['_migration']['multipart_legacy_recid'] = multipart_legacy_recid
    if 'legacy_recid' in first:
        del first['legacy_recid']
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
        volume=str(volume) if volume else None
    )


def link_and_create_multipart_volumes():
    """Link and create multipart volume records."""
    click.echo('Creating document volumes and multipart relations...')
    search = DocumentSearch().filter('term', _migration__is_multipart=True)

    for hit in search.scan():
        if 'legacy_recid' not in hit:
            continue
        multipart = get_multipart_by_legacy_recid(hit.legacy_recid)
        documents = create_multipart_volumes(
            hit.pid,
            hit.legacy_recid,
            hit._migration.volumes
        )
        for document in documents:
            if document and multipart:
                create_parent_child_relation(
                    multipart,
                    document,
                    current_app.config['MULTIPART_MONOGRAPH_RELATION'],
                    document['volume']
                )


def get_serials_by_child_recid(recid):
    """Search serials by children recid."""
    search = SeriesSearch().query(
        'bool',
        filter=[
            Q('term', mode_of_issuance='SERIAL'),
            Q('term', _migration__children=recid),
        ]
    )
    for hit in search.scan():
        yield Series.get_record_by_pid(hit.pid)


def get_migrated_volume_by_serial_title(record, title):
    """Get volume number by serial title."""
    for serial in record['_migration']['serials']:
        if serial['title'] == title:
            return serial.get('volume', None)
    raise DocumentMigrationError(
        'Unable to find volume number in record {} by title "{}"'.format(
            record['pid'],
            title
        )
    )


def link_documents_and_serials():
    """Link documents/multiparts and serials."""
    def link_records_and_serial(record_cls, search):
        for hit in search.scan():
            # Skip linking if the hit doesn't have a legacy recid since it
            # means it's a volume of a multipart
            if 'legacy_recid' not in hit:
                continue
            record = record_cls.get_record_by_pid(hit.pid)
            for serial in get_serials_by_child_recid(hit.legacy_recid):
                volume = get_migrated_volume_by_serial_title(
                    record,
                    serial['title']['title']
                )
                create_parent_child_relation(
                    serial,
                    record,
                    current_app.config['SERIAL_RELATION'],
                    volume
                )

    click.echo('Creating serial relations...')
    link_records_and_serial(
        Document,
        DocumentSearch().filter('term', _migration__has_serial=True)
    )
    link_records_and_serial(
        Series,
        SeriesSearch().filter('bool', filter=[
            Q('term', mode_of_issuance='MULTIPART_MONOGRAPH'),
            Q('term', _migration__has_serial=True),
        ])
    )


def validate_serial_records():
    """Validate that serials were migrated successfully.

    Performs the following checks:
    * Find duplicate serials
    * Ensure all children of migrated serials were migrated
    """
    def validate_serial_relation(serial, recids):
        relations = serial.relations.get().get('serial', [])
        if len(recids) != len(relations):
            click.echo(
                '[Serial {}] Incorrect number of children: {} '
                '(expected {})'.format(
                    serial['pid'],
                    len(relations),
                    len(recids)
                )
            )
        for relation in relations:
            child = Document.get_record_by_pid(
                relation['pid'],
                pid_type=relation['pid_type']
            )
            if 'legacy_recid' in child and child['legacy_recid'] not in recids:
                click.echo(
                    '[Serial {}] Unexpected child with legacy '
                    'recid: {}'.format(serial['pid'], child['legacy_recid'])
                )

    titles = set()
    search = SeriesSearch().filter('term', mode_of_issuance='SERIAL')
    for serial_hit in search.scan():
        # Store titles and check for duplicates
        if 'title' in serial_hit and 'title' in serial_hit.title:
            title = serial_hit.title.title
            if title in titles:
                current_app.logger.warning(
                    'Serial title "{}" already exists'.format(title))
            else:
                titles.add(title)
        # Check if any children are missing
        children = serial_hit._migration.children
        serial = Series.get_record_by_pid(serial_hit.pid)
        validate_serial_relation(serial, children)

    click.echo('Serial validation check done!')


def validate_multipart_records():
    """Validate that multiparts were migrated successfully.

    Performs the following checks:
    * Ensure all volumes of migrated multiparts were migrated
    """
    def validate_multipart_relation(multipart, volumes):
        relations = multipart.relations.get().get('multipart_monograph', [])
        titles = [volume['title'] for volume in volumes if 'title' in volume]
        count = len(set(v['volume'] for v in volumes))
        if count != len(relations):
            click.echo(
                '[Multipart {}] Incorrect number of volumes: {} '
                '(expected {})'.format(multipart['pid'], len(relations), count)
            )
        for relation in relations:
            child = Document.get_record_by_pid(
                relation['pid'],
                pid_type=relation['pid_type']
            )
            if child['title']['title'] not in titles:
                click.echo(
                    '[Multipart {}] Title "{}" does not exist in '
                    'migration data'.format(
                        multipart['pid'],
                        child['title']['title']
                    )
                )

    search = SeriesSearch().filter(
        'term',
        mode_of_issuance='MULTIPART_MONOGRAPH'
    )
    for multipart_hit in search.scan():
        # Check if any child is missing
        if 'volumes' in multipart_hit._migration:
            volumes = multipart_hit._migration.volumes
            multipart = Series.get_record_by_pid(multipart_hit.pid)
            validate_multipart_relation(multipart, volumes)

    click.echo('Multipart validation check done!')
