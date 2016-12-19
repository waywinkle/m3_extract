import pypyodbc
from properties import get_all_properties
import logging

logger = logging.getLogger(__name__)

PRIMARY_KEY_SQL = '''
SELECT SK.COLUMN_NAME
FROM {schema}.SYSINDEXES SI
INNER JOIN {schema}.SYSKEYS SK
  ON SI.INDEX_NAME = SK.INDEX_NAME
WHERE SI.IS_UNIQUE = 'U'
  AND SI.TABLE_NAME = '{table}'
'''
COLUMN_SQL = '''
SELECT COLUMN_NAME,
    ORDINAL_POSITION,
    DATA_TYPE,
    LENGTH
FROM {schema}.SYSCOLUMNS
WHERE TABLE_NAME = '{table}'
ORDER BY ORDINAL_POSITION
'''
TABLE_SQL = '''
SELECT TABLE_NAME
FROM SYSIBM.TABLES
WHERE TABLE_SCHEMA = 'M3FDBPRD'
  AND TABLE_TYPE = 'BASE TABLE'
'''
PROPS = get_all_properties('properties.json')


def get_metadata(table):
    columns = get_columns(table)
    logging.debug(columns)
    keys = get_primary_keys(table)
    logging.debug(keys)
    tables = get_tables()
    logging.debug(tables)


def get_tables(schema):
    connection = pypyodbc.connect(PROPS['connection_string'])
    cursor = connection.cursor()
    cursor.execute(TABLE_SQL.format(schema=schema))
    rows = cursor.fetchall()
    keys = []
    for row in rows:
        keys.append(row[0])
    cursor.close()
    connection.close()

    return keys


def get_columns(schema, table):
    connection = pypyodbc.connect(PROPS['connection_string'])
    cursor = connection.cursor()
    cursor.execute(COLUMN_SQL.format(schema=schema, table=table))
    rows = cursor.fetchall()
    keys = []
    for row in rows:
        record = {'column_name': row[0],
                  'ordinal_position': row[1],
                  'DATA_TYPE': row[2],
                  'LENGTH': row[3]}
        keys.append(record)
    cursor.close()
    connection.close()

    return keys


def get_primary_keys(schema, table):
    connection = pypyodbc.connect(PROPS['connection_string'])
    cursor = connection.cursor()
    cursor.execute(PRIMARY_KEY_SQL.format(schema=schema, table=table))
    rows = cursor.fetchall()
    keys = []
    for row in rows:
        keys.append(row[0])
    cursor.close()
    connection.close()

    return keys


if __name__ == '__main__':
    get_metadata('MITTRA')
