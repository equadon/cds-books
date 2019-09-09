# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2019 CERN.
#
# cds-books is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS-Books migrator CLI."""


from __future__ import absolute_import, print_function

import json
import os
import re

import click
import sqlalchemy
from flask import current_app
from flask.cli import with_appcontext

from cds_books.migrator.api import import_parent_record, \
    import_parents_from_file, import_records_from_dump, \
    link_and_create_multipart_volumes, link_documents_and_serials, \
    reindex_pidtype, validate_serials


@click.group()
def migrate():
    """CDS Books migrator commands."""


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
    import_records_from_dump(
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
    """Migrate parents serials, multiparts or keywords from dumps."""
    click.echo('Migrating {}s...'.format(rectype))
    import_parents_from_file(source, rectype=rectype, include=include)


@migrate.command()
@click.option('--dry-run', is_flag=True)
@with_appcontext
def relations(dry_run):
    """Setup relations."""
    link_and_create_multipart_volumes(dry_run)
    link_documents_and_serials(dry_run)

    if dry_run:
        click.echo('No changes were made. '
                   'Disable dry-run to update the database.')
    else:
        reindex_pidtype('docid')
        reindex_pidtype('serid')


@migrate.group()
def check():
    """Check if there are any issues with the migration."""


@check.command(name='serials')
@with_appcontext
def check_serials():
    """Check migrated serials for errors."""
    validate_serials()
