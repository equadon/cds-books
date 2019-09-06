from __future__ import absolute_import, print_function

import json
import os
import re
import sqlalchemy

import click
from flask import current_app
from flask.cli import with_appcontext
from invenio_app_ils.records.api import Document, Series, Keyword
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

from cds_books.migrator.api import link_and_create_multipart_volumes
from cds_books.migrator.errors import LossyConversion
from cds_books.migrator.records import CDSParentRecordDumpLoader


@click.group()
def migrate():
    """CDS Books migrator commands."""


def reindex_documents():
    """Reindex all documents."""
    click.echo('Indexing all documents...')
    cli = create_cli()
    runner = current_app.test_cli_runner()
    runner.invoke(
        cli,
        'index reindex --pid-type docid --yes-i-know',
        catch_exceptions=True
    )
    runner.invoke(cli, 'index run', catch_exceptions=False)
    click.echo('All documents successfully indexed!')


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


def load_parents_from_file(dump_file, rectype, include):
    """Load parent records from file."""
    model, provider = model_provider_by_rectype(rectype)
    include_keys = None if include is None else include.split(',')
    with click.progressbar(json.load(dump_file).items()) as bar:
        records = []
        for key, parent in bar:
            if include_keys is None or key in include_keys:
                record = load_parent_record(parent, model, provider)
                click.echo('Loaded {} with PID "{}"...'.format(
                    rectype,
                    record["pid"]
                ))
                records.append(record)
    # Index all new parent records
    bulk_index_records(records)


def load_parent_record(dump, model, pid_provider):
    try:
        record = CDSParentRecordDumpLoader.create(dump, model, pid_provider)
        db.session.commit()
        return record
    except Exception:
        db.session.rollback()
        raise


def load_records_from_dump(sources, source_type, eager, include):
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
    reindex_documents()


@migrate.command()
@click.argument('sources', type=click.File('r'), nargs=-1)
@click.option(
    '--source-type',
    '-t',
    type=click.Choice(['json', 'marcxml']),
    default='marcxml',
    help='Whether to use JSON or MARCXML.')
@click.option(
    '--include',
    '-i',
    help='Comma-separated list of legacy recids to include in the import',
    default=None)
@with_appcontext
def documents(sources, source_type, include):
    """Migrate documents from CDS legacy."""
    load_records_from_dump(
        sources=sources,
        source_type=source_type,
        eager=True,
        include=include
    )


@migrate.command()
@click.argument('rectype', nargs=1, type=str)
@click.argument('source', nargs=1, type=click.File())
@click.option(
    '--include',
    '-i',
    help='Comma-separated list of legacy recids (for multiparts) or serial '
         'titles to include in the import',
    default=None)
@with_appcontext
def parents(rectype, source, include):
    """Migrate parents (serials, multiparts or keywords) from dumps."""
    load_parents_from_file(source, rectype=rectype, include=include)


@migrate.command()
@click.option('--dry-run', is_flag=True)
@with_appcontext
def relations(dry_run):
    """Setup relations."""
    link_and_create_multipart_volumes(dry_run)
    if dry_run:
        click.echo('No changes were made. Disable dry-run to update the database.')
    else:
        reindex_documents()
