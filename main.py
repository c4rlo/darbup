#!/usr/bin/python3

import config
from archive import ArchiveSet
from blockrun import block_run
from errors import BackupError

import logging
from logging.handlers import RotatingFileHandler
import datetime

def main():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

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
    if not arcset:
        logging.info('Starting initial full backup: ' + cfg.name)
        backup(False, cfg, now, arcset)
    elif is_it_time(cfg.full_intvl, now, arcset):
        logging.info('Starting full backup: ' + cfg.name)
        backup(False, cfg, now, arcset)
    elif is_it_time(cfg.incr_intvl, now, arcset):
        logging.info('Starting incremental backup: ' + cfg.name)
        backup(True, cfg, now, arcset)

def is_it_time(intvl, now, arcset):
    return intvl(arcset.latest().timestamp, now)

def backup(is_incr, cfg, now, arcset):
    command = [ '/usr/bin/dar', '-c', '-' ]
    if is_incr:
        command.extend(( '-A', arcset.latest().basepath() ))
    command.extend(cfg.dar_args)
    def cleaner():
        return cfg.rmpolicy(arcset, now)
    block_run(command, arcset.basepath_for_time(now, is_incr), cfg.capacity,
              cleaner)

if __name__ == '__main__':
    main()
