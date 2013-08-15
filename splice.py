import ctypes
import ctypes.util
from ctypes import c_int, c_size_t, c_ssize_t
import errno
import os

def make_splice():
    libc_name = ctypes.util.find_library('c')
    libc = ctypes.CDLL(libc_name, use_errno=True)
    c_splice = libc.splice
    c_splice.restype = c_ssize_t

    def splice(fd_in, fd_out, length):
        while True:
            res = c_splice(c_int(fd_in), None, c_int(fd_out), None,
                           c_size_t(length), 0)
            if res == -1:
                errno_ = ctypes.get_errno()
                if errno_ == errno.EINTR:
                    continue
                raise OSError(errno_, os.strerror(errno_))
            return res
    return splice

splice = make_splice()
del make_splice
