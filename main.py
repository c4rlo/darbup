#!/usr/bin/python3

import config
from archive import ArchiveSet
from errors import BackupError

import logging
from logging.handlers import RotatingFileHandler
import datetime

def main():
    logger = logging.getLogger()

    conf = config.Config()
    for cfg in conf.instances:
        log_handler = RotatingFileHandler(cfg.logfilename)
        logger.addHandler(log_handler)
        log_formatter = logging.Formatter(style='{')
        log_handler.setFormatter(log_formatter)
        try:
            run(cfg)
        except BackupError as e:
            logger.error(str(e))
        except Exception as e:
            logger.exception(e)
        logger.removeHandler(log_handler)

def run(cfg):
    now = datetime.datetime.now()
    arcset = ArchiveSet(cfg.name, cfg.dest_dir)
    if is_it_time(cfg.full_intvl, now, arcset):
        logger.info('Starting full backup: {}', cfg.name)
    elif is_it_time(cfg.incr_intvl, now, arcset):
        logger.info('Starting incremental backup: {}', cfg.name)

def is_it_time(intvl, now, arcset):
    return intvl(arcset.latest().timestamp, now)

if __name__ == '__main__':
    main()
