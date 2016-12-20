from properties import get_all_properties
import logging
from sql import get_columns, get_primary_keys, get_tables
import os
import json

logger = logging.getLogger(__name__)

PROPS = get_all_properties('properties.json')


def get_metadata(schema):
    active = get_active_tables()
    active_list = [i for i, j in active.items()]
    tables = get_tables(schema, active_list)
    metadata = dict()

    for table in tables:
        table_meta = dict()
        table_meta['columns'] = get_columns(schema, table)
        table_meta['keys'] = get_primary_keys(schema, table)
        table_meta['active'] = active.get(table, False)
        for column in table_meta['columns']:
            if column['column_name'][2:6] == 'LMDT':
                table_meta['change_column'] = column['column_name']

        metadata[table] = table_meta

    return metadata


def get_active_tables(
    default_path='active_tables.json',
):

    path = default_path
    if os.path.exists(path):
        with open(path, 'rt') as f:
            return json.load(f)
    else:
        return {}
