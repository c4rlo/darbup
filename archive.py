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

import re, os, os.path, logging
from datetime import datetime
from operator import attrgetter

from errors import BackupError

_ARCHIVE_BASENAME_SUFFIX_PAT = \
        r'-(\d\d\d\d)-(\d\d)-(\d\d)-(\d\d)(\d\d)-(full|incr)\.1\.dar$'

_BASENAME_SUFFIX_RE = re.compile(_ARCHIVE_BASENAME_SUFFIX_PAT)


class Archive:
    def __init__(self, basedir, basename, timestamp, is_incr, size):
        self._basedir = basedir
        self._basename = basename
        self.timestamp = timestamp
        self.is_incremental = is_incr
        self.size = size
        self._prev = self._next = None
        self.is_current = False

    def path(self):
        return os.path.join(self._basedir, self._basename)

    def basepath(self):
        path = self.path()
        assert path.endswith('.1.dar')
        return path[:-6]

    def prev(self): return self._prev

    def next(self): return self._next

    def has_dependent(self):
        return self._next and self._next.is_incremental


class ArchiveSet:
    def __init__(self, name, path):
        self._name = name
        self._basedir = path
        archives = [ ]
        for fn in os.listdir(path):
            archive = self._archive_at_path(fn)
            if archive:
                logging.debug('Found existing archive {}'.format(fn))
                archives.append(archive)
        archives.sort(key=attrgetter('timestamp'))
        if len(archives) > 0 and archives[0].is_incremental:
            raise BackupError('Oldest archive {} is incremental'
                              .format(archive.path()))
        self._first = None
        self._last = None
        self._count = 0
        self._total_size = 0
        for arc in archives:
            self._append(arc)

    def __iter__(self):
        curr = self._first
        while curr:
            yield curr
            curr = curr._next

    def __len__(self):
        return self._count

    def __str__(self):
        return '{} bytes in {} archives (base name "{}" under {})'.format(
            self._total_size, self._count, self._name, self._basedir)

    def total_size(self):
        return self._total_size

    def append_current(self, timestamp, is_incr):
        basename = "{}-{:%Y-%m-%d-%H%M}-{}.1.dar".format(
            self._name, timestamp,
            'incr' if is_incr else 'full')
        archive = Archive(self._basedir, basename, timestamp, is_incr, None)
        archive.is_current = True
        self._append(archive)
        return os.path.join(self._basedir, basename)

    def _append(self, archive):
        if self._first is None:
            self._first = archive
        else:
            self._last._next = archive
            archive._prev = self._last
        self._last = archive
        self._count += 1
        if archive.size is not None:
            self._total_size += archive.size

    def remove(self, archive):
        if self._first == archive: self._first = archive._next
        if self._last == archive: self._last = archive._prev
        if archive._prev: archive._prev._next = archive._next
        if archive._next: archive._next._prev = archive._prev
        archive._prev = archive._next = None
        self._count -= 1
        if archive.size is not None:
            self._total_size -= archive.size

    def first(self):
        return self._first

    def latest(self):
        return self._last

    def _archive_at_path(self, basename):
        if basename.startswith(self._name):
            m = _BASENAME_SUFFIX_RE.match(basename[len(self._name):])
            if m:
                return Archive(self._basedir, basename,
                               datetime(int(m.group(1)), int(m.group(2)),
                                        int(m.group(3)), int(m.group(4)),
                                        int(m.group(5))),
                               m.group(6) == 'incr',
                               os.path.getsize(os.path.join(self._basedir,
                                                            basename)))
        return None
