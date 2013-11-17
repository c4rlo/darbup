#!/usr/bin/python3 -O

# Copyright 2013 Carlo Teubner
#
# This file is part of darbup.
#
# darbup is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# darbup is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with darbup.  If not, see <http://www.gnu.org/licenses/>.

import config
from archive import ArchiveSet
from procwrite import proc_write
from cleaner import make_cleaner
from errors import BackupError, NoRemovalCandidatesError, TerminatedSignal
from errors import exc_str

import sys, datetime, os, os.path, pwd, signal
import argparse, logging
from logging.handlers import RotatingFileHandler


def main():
    signal.signal(signal.SIGTERM, handle_sigterm)

    euid = os.geteuid()
    pw = pwd.getpwuid(euid)

    if euid == 0:  # I am root
        default_config = '/etc/darbup.conf'
        lock_filename = '/run/darbup.lock'
    else:
        if not pw.pw_dir:
            sys.stderr.write('error: user {} has no home directory'.format(
                pw.pw_name))
            return 1
        darbup_dir = os.path.join(pw.pw_dir, '.darbup')
        default_config = os.path.join(darbup_dir, 'config')
        lock_filename = os.path.join(darbup_dir, 'lock')
        try:
            os.mkdir(darbup_dir)
        except FileExistsError:
            pass

    parser = argparse.ArgumentParser(description='regular backup using dar')
    parser.add_argument('--full', action='store_true', help='do full backup')
    parser.add_argument('--incr', action='store_true',
                        help='do incremental backup')
    parser.add_argument('-c', '--config', metavar='FILENAME',
                        help='configuration file to use (default: {})' \
                        .format(default_config), default=default_config)
    parser.add_argument('-l', '--loglevel', metavar='LEVEL',
                        help='logging level (default: INFO)',
                        choices=('DEBUG','INFO','WARNING','ERROR','CRITICAL'),
                        default='INFO')
    args = parser.parse_args()
    if args.full and args.incr:
        sys.stderr.write(parser.format_usage())
        sys.stderr.write('error: only one of --full, --incr may be given\n')
        return 2

    have_lock = False

    try:
        with open(lock_filename, 'a+') as lock_file:
            lock_file.seek(0)
            try:
                os.lockf(lock_file.fileno(), os.F_TLOCK, 0)
            except BlockingIOError:
                other_pid = lock_file.read().rstrip()
                sys.stderr.write('Another instance of darbup is already '
                                 'running as PID {} for user {}\n'
                                 .format(other_pid, pw.pw_name))
                return 1

            have_lock = True
            lock_file.truncate(0)
            lock_file.write('{}\n'.format(os.getpid()))
            lock_file.flush()

            darbup(args, default_config)
    finally:
        if have_lock:
            os.remove(lock_filename)


def darbup(args, default_config):
    logger = logging.getLogger()
    logger.setLevel(args.loglevel)

    errlogHandler = logging.StreamHandler(sys.stderr)
    errlogHandler.setLevel(logging.ERROR)
    logger.addHandler(errlogHandler)

    if not os.path.exists(args.config) and args.config == default_config:
        config.write_default_config(default_config)
        return 1

    try:
        conf = config.Config(args.config)
    except Exception as e:
        sys.stderr.write('Failed to read configuration: {}\n'
                         .format(exc_str(e)))
        return 1

    for cfg in conf.instances:
        os.makedirs(os.path.dirname(cfg.logfilename), exist_ok=True)
        log_handler = LogFileHandler(cfg.logfilename,
                                     backupCount=cfg.logsbackupcount)
        logger.addHandler(log_handler)
        log_formatter = logging.Formatter(
            '{asctime} {levelname[0]}{levelname[0]} {message}', style='{')
        log_handler.setFormatter(log_formatter)
        try:
            run(cfg, args.full, args.incr)
        except (BackupError, OSError) as e:
            logging.error(str(e))
        except (KeyboardInterrupt, TerminatedSignal) as e:
            logging.error(exc_str(e))
            return 1
        except Exception as e:
            logging.exception(e)
            raise
        finally:
            logger.removeHandler(log_handler)


def run(cfg, force_full, force_incr):
    clean_parts(cfg.dest_dir)
    now = datetime.datetime.now()
    arcset = ArchiveSet(cfg.name, cfg.dest_dir)

    if force_incr:
        if not arcset:
            raise BackupError('Cannot run incremental backup: no archives '
                              'exist yet')
        backup(True, cfg, now, arcset)
    elif force_full or not arcset:
        backup(False, cfg, now, arcset)
    else:
        latest_time = arcset.latest().timestamp
        if cfg.full_intvl(latest_time, now):
            backup(False, cfg, now, arcset)
        elif cfg.incr_intvl(latest_time, now):
            backup(True, cfg, now, arcset)
        else:
            logging.debug('Not time for next backup yet: ' + cfg.name)


def backup(is_incr, cfg, now, arcset):
    logging.info('Existing archives: {!s}'.format(arcset))
    type_word = 'incremental' if is_incr else 'full'
    logging.info('Starting {} backup: {}'.format(type_word, cfg.name))

    def make_command():
        cmd = [ '/usr/bin/dar', '-c', '-' ]
        if is_incr:
            cmd.extend(( '-A', arcset.latest().basepath() ))
        cmd.extend(cfg.dar_args)
        return cmd

    command = make_command()

    if cfg.ignore_changing_files:
        good_exit_codes = (0, 11)
        # The dar manpage documents exit code 11 as the one that is returned if
        # files changed while reading them, leading to potentially bad data in
        # the resulting archive.
    else:
        good_exit_codes = (0,)

    dest_path = arcset.append_current(now, is_incr)
    # We must add the archive to the set now, so that the removal policy is
    # aware of it (i.e. if it's incremental, we must not remove the previous
    # archive)

    dest_path_temp = dest_path + '.part'
    cleaner = make_cleaner(cfg.rmpolicy, arcset, now)

    while True:
        try:
            status, num_bytes = proc_write(command, dest_path_temp,
                                      cfg.capacity - arcset.total_size(),
                                      cleaner, good_exit_codes)
            # Note: if this returns a bad (nonzero) status, or raises and
            # exception, the file at 'dest_temp_path' has already been removed.
            break
        except NoRemovalCandidatesError:
            # Remove the current archive, and see if we are now able to delete
            # an archive.  It's possible that it was previously impossible
            # because the current archive was dependent on the previous one
            # (i.e. it was incremental).
            # Note: 'cleaner()' either successfully removes an old archive, or
            # throws an error. Hence, we cannot get into an infinite loop.
            arcset.remove(arcset.latest())
            cleaner()
            # As 'arcset.latest()' may have changed, re-genereate the dar
            # command
            command = make_command()
            arcset.append_current(now, is_incr)

    if status in good_exit_codes:
        os.rename(dest_path_temp, dest_path)
        logging.info('Created new {} backup at {} ({} bytes)'.format(
            type_word, dest_path, num_bytes))
    else:
        logging.error('Failed to create new {} backup at {}: dar failed ({}) '
                      'after writing {} bytes'.format(
                          type_word, dest_path, status, num_bytes))


def clean_parts(path):
    for fn in os.listdir(path):
        if fn.endswith('.dar.part'):
            fullpath = os.path.join(path, fn)
            os.remove(fullpath)
            logging.info('Removed left-over partial backup {}'.format(fullpath))


class LogFileHandler(RotatingFileHandler):
    def __init__(self, filename, **kwargs):
        self.__doneInitialRollover = not os.path.exists(filename)
        RotatingFileHandler.__init__(self, filename, **kwargs)

    def emit(self, record):
        if not self.__doneInitialRollover:
            self.doRollover()
            self.__doneInitialRollover = True
        return RotatingFileHandler.emit(self, record)


def handle_sigterm(sig, stk):
    raise TerminatedSignal()


if __name__ == '__main__':
    if sys.version_info < (3, 3):
        sys.exit('Python >= 3.3 required')
    status = main()
    sys.exit(status)
