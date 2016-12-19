import logging
from log_setup import setup_logging
from sql_metadata import get_metadata
import sys
import traceback


def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.debug('starting')
    get_metadata('MITTRA')



if __name__ == "__main__":
    main()
