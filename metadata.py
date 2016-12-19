from properties import get_all_properties
import logging

logger = logging.getLogger(__name__)





PROPS = get_all_properties('properties.json')


def get_metadata(table):
    columns = get_columns(table)
    logging.debug(columns)
    keys = get_primary_keys(table)
    logging.debug(keys)
    tables = get_tables()
    logging.debug(tables)