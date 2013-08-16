import os, subprocess
from splice import splice

def block_run(command, filename, limit, callback):
    with open(filename, 'w') as f:
        proc = subprocess.Popen(command, stdout=subprocess.PIPE)
        total_num_written = 0
        try:
            while True:
                if limit <= 0:
                    limit = callback()
                    print("new limit is {}".format(limit))
                try:
                    num_written = splice(proc.stdout.fileno(), f.fileno(),
                                         limit)
                    print("splice wrote {} bytes".format(num_written))
                except OSError as e:
                    print(type(e), e)
                    status = None
                    try:
                        status = proc.wait(5)
                    except subprocess.TimeoutExpired:
                        proc.terminate()
                    return status, total_num_written
                total_num_written += num_written
                limit -= num_written
                if num_written == 0:
                    print("waiting for process to exit")
                    status = proc.wait()
                    return status, total_num_written
        except:
            print("terminating process")
            proc.terminate()
            raise

def cleaner():
    print("cleaner here")
    return 1024*1024

cmd = '/usr/bin/dar', '-c', '-', '-R', '/boot', '-I', 'vmlinuz-3*'
filename = 'junk'
limit = 1024*1024

status, numwritten = block_run(cmd, filename, limit, cleaner)
print("Exit status {}.  Wrote {} byte(s).".format(status, numwritten))
