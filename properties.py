import os
import json


def get_all_properties(property_file):
    with open(get_file_location(property_file)) as json_file:
        properties = json.load(json_file)

    return properties


def get_property(property_file, json_property):
    properties = get_all_properties(property_file)
    return properties[json_property]


def get_file_location(file_name):
    __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))

    return os.path.join(__location__, file_name)
