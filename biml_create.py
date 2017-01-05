from lxml import etree
from properties import get_property
import json
from biml import Biml

PROP = 'properties.json'


def get_base():
    with open(get_property(PROP, 'biml_base')) as base_file:
        return etree.parse(base_file)


if __name__ == "__main__":
    with open('metadata.json') as meta_file:
        meta = json.load(meta_file)
    file = 'MITLOC'
    file_data = meta[file]

    b = Biml(file, file_data['columns'], file_data['keys'], file_data['change_column'])
    print(etree.tostring(b._conversion()))
    print(etree.tostring(b._variables()))
    print(b.lookup_sql)
    print(etree.tostring(b._lookup_inputs()))
    print(etree.tostring(b._lookup_outputs()))
    print(b._filter_columns())
    print(etree.tostring(b._destination_columns()))

