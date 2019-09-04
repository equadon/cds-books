from __future__ import absolute_import, print_function

import json
import os
import re

import click
from flask import current_app
from flask.cli import with_appcontext
from invenio_app_ils.records.api import Document, Series, Keyword
from invenio_app_ils.pidstore.providers import DocumentIdProvider, \
    SeriesIdProvider
from invenio_db import db
from invenio_indexer.api import RecordIndexer
from invenio_migrator.cli import _loadrecord, dumps
from invenio_pidstore.errors import PIDAlreadyExists
from invenio_pidstore.models import PersistentIdentifier
from invenio_records import Record

from cds_books.migrator.records import CDSParentRecordDumpLoader


def index_documents():
    """Index all documents in the database."""
    # TODO: implement
    pass


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
    """Import parent records from file."""
    model, provider = model_provider_by_rectype(rectype)
    include_keys = None if include is None else include.split(',')
    with click.progressbar(json.load(dump_file).items()) as bar:
        records = []
        for key, parent in bar:
            if include_keys is None or key in include_keys:
                record = import_parent_record(parent, model, provider)
                click.echo('Imported serial with PID "{}"...'.format(record["pid"]))
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


def import_records(sources, source_type, eager, recids):
    """Load records."""
    recids = recids if recids is None else recids.split(',')
    for idx, source in enumerate(sources, 1):
        click.echo('Loading dump {0} of {1} ({2})'.format(
            idx, len(sources), source.name))
        data = json.load(source)
        with click.progressbar(data) as records:
            for item in records:
                if recids is None or str(item['recid']) in recids:
                    try:
                        _loadrecord(item, source_type, eager=eager)
                        click.echo('Imported record with legacy recid: {}'.format(
                            item['recid']))
                    except PIDAlreadyExists:
                        current_app.logger.warning(
                            "migration: report number associated with multiple"
                            "recid. See {0}".format(item['recid']))
    # We don't get the record back from _loadrecord so re-index all documents
    index_documents()


@dumps.command()
@click.argument('sources', type=click.File('r'), nargs=-1)
@click.option(
    '--source-type',
    '-t',
    type=click.Choice(['json', 'marcxml']),
    default='json',
    help='Whether to use JSON or MARCXML.')
@click.option(
    '--recids',
    '-r',
    help='Record ID(s) to load (NOTE: will load only those records).',
    default=None)
@with_appcontext
def load(sources, source_type, recids):
    """Load records migration dump."""
    import_records(sources=sources, source_type=source_type, eager=True,
                   recids=recids)


@dumps.command()
@click.argument('rectype', nargs=1, type=str)
@click.argument('source', nargs=1, type=click.File())
@click.option(
    '--include',
    '-i',
    help='Comma-separated list of legacy recids (for multiparts) or serial '
         'titles to include in the import',
    default=None)
@with_appcontext
def loadparents(rectype, source, include):
    """Load records migration dump."""
    import_parents_from_file(source, rectype=rectype, include=include)
