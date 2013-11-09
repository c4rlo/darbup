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

import ctypes, ctypes.util
from ctypes import c_int, c_size_t, c_ssize_t
import os, errno

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
