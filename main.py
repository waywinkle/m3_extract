import logging
from log_setup import setup_logging
from metadata import get_metadata
import os, errno
import json


def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.debug('starting')
    meta = get_metadata('M3FDBPRD')
    silent_remove('metadata.json')
    with open('metadata.json', 'w') as outfile:
        json.dump(meta, outfile)


def silent_remove(filename):
    try:
        os.remove(filename)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise

if __name__ == "__main__":
    main()
