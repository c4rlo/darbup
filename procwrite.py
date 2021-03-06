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

from errors import exc_str

import subprocess, signal, logging, threading, io, os
from splice import splice

def proc_write(command, filename, limit, cleaner, good_exit_codes=(0,)):
    logging.debug('Starting process {}, writing to {}, max {} bytes'
                  .format(command, filename, limit))
    success = False
    outfile = os.open(filename, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
    stderr_logger = None
    try:
        stderr_logger = _LoggerThread()
        status, numbytes = \
            _proc_write(command, outfile, limit, cleaner, stderr_logger)
        if status in good_exit_codes:
            success = True
    finally:
        if stderr_logger and stderr_logger.is_started:
            stderr_logger.join()
            logging.debug('Joined subprocess logger thread')
        os.close(outfile)
        if not success:
            os.remove(filename)
    return _interpret_exit_status(status), numbytes

_KILL_TIMEOUT_SECS = 3

def _proc_write(command, outfile, limit, cleaner, stderr_logger):
    proc = subprocess.Popen(command, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    proc_desc = '{} (pid {})'.format(
        command if isinstance(command, str) else command[0], proc.pid)
    proc_stdout = proc.stdout.fileno()
    stderr_logger.startLogging(proc.stderr, proc_desc)
    total_num_written = 0
    num_splice_calls = 0
    try:
        while True:
            if limit <= 0:
                logging.debug('Ran out of space: {} bytes left'.format(limit))
                limit += cleaner()
                logging.debug('After cleanup, new capacity is {} bytes'
                              .format(limit))
            try:
                num_splice_calls += 1
                eff_limit = min(limit, 2**30)  # avoid int overflows
                num_written = splice(proc_stdout, outfile, eff_limit)
            except OSError as e:
                logging.error(exc_str(e))
                status = None
                logging.info('Waiting for {} to exit'.format(proc_desc))
                try:
                    status = proc.wait(_KILL_TIMEOUT_SECS)
                    logging.debug('{} exited with status {}'.format(proc_desc,
                                                                    status))
                except subprocess.TimeoutExpired:
                    logging.warning('{} timed out: terminating'.format(
                        proc_desc))
                    _kill(proc, proc_desc)
                return status, total_num_written
            total_num_written += num_written
            limit -= num_written
            if num_written == 0:
                logging.debug('Finished: wrote {} bytes in {} splice() '
                              'calls; waiting for {} to exit'.format(
                                  total_num_written, num_splice_calls,
                                  proc_desc))
                status = proc.wait()
                logging.debug('{} exited with status {}'.format(proc_desc,
                                                                status))
                return status, total_num_written
    except:
        logging.warning('Terminating {}'.format(proc_desc))
        _kill(proc, proc_desc)
        raise

class _LoggerThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.is_started = False

    def startLogging(self, f, prefix):
        self.f = f
        self.prefix = prefix
        self.is_started = True
        self.start()

    def run(self):
        for line in io.TextIOWrapper(self.f, errors='replace'):
            logging.info('{}: {}'.format(self.prefix, line.rstrip('\r\n')))
        logging.debug('Exiting logger thread for {}'.format(self.prefix))

def _kill(proc, proc_desc):
    proc.terminate()
    logging.info('Requested {} to exit'.format(proc_desc))
    try:
        proc.wait(_KILL_TIMEOUT_SECS)
    except subprocess.TimeoutExpired:
        logging.warning('Timed out; killing {}'.format(proc_desc))
        proc.kill()
    except BaseException as e:
        logging.warning('{}. Killing {}'.format(exc_str(e), proc_desc))
        proc.kill()

def _interpret_exit_status(status):
    if status is None:
        return '<unknown>'
    elif status == 0:
        return 0
    elif status > 0:
        return 'bad exit status ' + str(status)
    else:
        signalno = -status
        for name, value in vars(signal).items():
            if name.startswith('SIG') and not name.startswith('SIG_') \
               and value == signalno:
                return 'killed by signal ' + name[3:]
        return 'killed by unknown signal ' + signalno
