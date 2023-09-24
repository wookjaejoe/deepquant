# noinspection SpellCheckingInspection
__author__ = 'wookjae.jo'

import logging
import logging.handlers
import os
import sys
from datetime import datetime

__debug = False

LOG_FOLDER = os.path.abspath('.log')
os.makedirs(LOG_FOLDER, exist_ok=True)

DEFAULT_LOG_FORMATTER = logging.Formatter(
    fmt="[%(asctime)s.%(msecs)03d] [%(levelname)-4s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


def _create_stream_handler(level: int = logging.INFO):
    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setLevel(level)
    stream_handler.setFormatter(DEFAULT_LOG_FORMATTER)
    return stream_handler


def _create_file_handler(level: int = logging.DEBUG):
    date = datetime.now().strftime('%Y%m%d')
    file_handler = logging.handlers.RotatingFileHandler(
        filename=os.path.join(LOG_FOLDER, f'{date}.log'),
        encoding='utf-8',
        maxBytes=4 * 1024 * 1024,
        backupCount=2)
    file_handler.setLevel(level)
    file_handler.setFormatter(DEFAULT_LOG_FORMATTER)
    return file_handler


def init(level: int = logging.INFO):
    logging.basicConfig(level=level)
    root_logger = logging.getLogger()
    root_logger.handlers = [
        _create_stream_handler(level=level),
        _create_file_handler(level=level)
    ]


logger = logging.getLogger()
