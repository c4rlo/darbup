import subprocess, signal, logging, threading, io, os
from splice import splice

def block_run(command, filename, limit, cleaner):
    logging.debug('Starting process {}, writing to {}, max {} bytes' \
                  .format(command, filename, limit))
    try:
        outfile = open(filename, 'w')
        stderr_logger = _LoggerThread()
        success = False
        status, numbytes = \
            _block_run(command, outfile, limit, cleaner, stderr_logger)
        if status == 0:
            success = True
    finally:
        if stderr_logger.is_started:
            stderr_logger.join()
            logging.debug('Joined subprocess logger thread')
        outfile.close()
        if not success:
            os.remove(filename)
    return status, numbytes

def _block_run(command, outfile, limit, cleaner, stderr_logger):
    proc = subprocess.Popen(command, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    proc_desc = '{} (pid {})'.format(
        command if isinstance(command, str) else command[0], proc.pid)
    stderr_logger.startLogging(proc.stderr, proc_desc)
    total_num_written = 0
    num_splice_calls = 0
    try:
        while True:
            if limit <= 0:
                logging.debug('Ran out of space: {} bytes left'.format(limit))
                limit += cleaner()
                logging.debug('After cleanup, new capacity is {} bytes' \
                              .format(limit))
            try:
                num_splice_calls += 1
                eff_limit = min(limit, 2**30)  # avoid int overflows
                num_written = splice(proc.stdout.fileno(), outfile.fileno(),
                                     eff_limit)
                # logging.debug('splice with limit {} -> {} bytes'.format(
                #                 eff_limit, num_written))
            except OSError as e:
                logging.error(str(e))
                status = None
                try:
                    status = proc.wait(5)
                    logging.debug('{} exited with status {}'.format(proc_desc,
                                                                    status))
                except subprocess.TimeoutExpired:
                    logging.warning('{} timed out: terminating'.format(
                        proc_desc))
                    proc.terminate()
                return _interpret_exit_status(status), total_num_written
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
                return _interpret_exit_status(status), total_num_written
    except:
        logging.warning('Terminating {}'.format(proc_desc))
        proc.terminate()
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
