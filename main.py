#!/usr/bin/python3

import config
from archive import ArchiveSet
from blockrun import block_run
from cleaner import make_cleaner
from errors import BackupError, NoRemovalCandidatesError

import sys, datetime, os, os.path
import argparse, logging
from logging.handlers import RotatingFileHandler

def main():
    default_config = os.path.join(os.environ['HOME'], '.regudar', 'config')

    parser = argparse.ArgumentParser(description='regular backup using dar')
    parser.add_argument('-c', '--config', metavar='FILENAME',
                        help='configuration file to use (default: '
                            '$HOME/.regudar/config)',
                        default=default_config)
    parser.add_argument('-l', '--loglevel', metavar='LEVEL',
                        help='logging level (default: INFO)',
                        choices=('DEBUG','INFO','WARNING','ERROR','CRITICAL'),
                        default='INFO')
    args = parser.parse_args()

    logger = logging.getLogger()
    logger.setLevel(args.loglevel)

    try:
        conf = config.Config(args.config)
    except Exception as e:
        sys.stderr.write('Failed to read configuration: {!s}\n'.format(e))
        return 1

    for cfg in conf.instances:
        logfile_exists = os.path.exists(cfg.logfilename)
        log_handler = RotatingFileHandler(cfg.logfilename, backupCount=1)
        logger.addHandler(log_handler)
        if logfile_exists:
            log_handler.doRollover()
        log_formatter = logging.Formatter(
            '{asctime} {levelname[0]}{levelname[0]} {message}', style='{')
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
    logging.info('Existing archives: {!s}'.format(arcset))
    if not arcset:
        logging.info('Starting initial full backup: ' + cfg.name)
        backup(False, cfg, now, arcset)
    else:
        latest_time = arcset.latest().timestamp
        if cfg.full_intvl(latest_time, now):
            logging.info('Starting full backup: ' + cfg.name)
            backup(False, cfg, now, arcset)
        elif cfg.incr_intvl(latest_time, now):
            logging.info('Starting incremental backup: ' + cfg.name)
            backup(True, cfg, now, arcset)
        else:
            logging.debug('Not time for next backup yet: ' + cfg.name)

def backup(is_incr, cfg, now, arcset):
    def make_command():
        cmd = [ '/usr/bin/dar', '-c', '-' ]
        if is_incr:
            cmd.extend(( '-A', arcset.latest().basepath() ))
        cmd.extend(cfg.dar_args)
        return cmd

    command = make_command()

    dest_path = arcset.append_current(now, is_incr)
    # We must add the archive to the set now, so that the removal policy is
    # aware of it (i.e. if it's incremental, we must not remove the previous
    # archive)

    dest_path_temp = dest_path + '.part'
    cleaner = make_cleaner(cfg.rmpolicy, arcset, now)

    while True:
        try:
            status, num_bytes = block_run(command, dest_path_temp,
                                      cfg.capacity - arcset.total_size(),
                                      cleaner)
            break
        except NoRemovalCandidatesError:
            arcset.remove(arcset.latest())
            # Remove the current archive and see if we are now able to delete an
            # archive.  It's possible that it was previously impossible because
            # the current archive was dependent on the previous one (i.e. it was
            # incremental).
            cleaner()
            command = make_command()
            arcset.append_current(now, is_incr)

    if status == 0:
        os.rename(dest_path_temp, dest_path)
        logging.info('Created new {} backup at {} ({} bytes)'.format(
            'incremental' if is_incr else 'full',
            dest_path, num_bytes))
    else:
        logging.error('Failed to create new {} backup at {}: dar failed ({}) '
                      'after writing {} bytes'.format(
                          'incremental' if is_incr else 'full',
                          dest_path, status, num_bytes))

if __name__ == '__main__':
    status = main()
    sys.exit(status)
