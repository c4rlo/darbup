import re, os, os.path
from datetime import datetime
from operator import attrgetter

from errors import BackupError

_ARCHIVE_BASENAME_SUFFIX_PAT = \
        r'-(\d\d\d\d)-(\d\d)-(\d\d)-(\d\d)(\d\d)-(full|incr)\.1\.dar$'

_BASENAME_SUFFIX_RE = re.compile(_ARCHIVE_BASENAME_SUFFIX_PAT)


class Archive:
    def __init__(self, basedir, basename, match):
        self._basedir = basedir
        self._basename = basename
        self.timestamp = datetime(int(match.group(1)), int(match.group(2)),
                                  int(match.group(3)), int(match.group(4)),
                                  int(match.group(5)))
        self.is_incremental = (match.group(6) == 'incr')
        self.size = os.path.getsize(self.path())
        self._prev = self._next = None

    def path(self):
        return os.path.join(self._basedir, self._basename)

    def prev(self): return self._prev

    def next(self): return self._next

    def has_dependent(self):
        return self._next and self._next.is_incremental


class ArchiveSet:
    def __init__(self, name, path):
        self._name = name
        archives = [ ]
        for fn in os.listdir(path):
            archive = self._archive_at_path(path, fn)
            if archive:
                archives.append(archive)
        archives.sort(key=attrgetter('timestamp'))
        if len(archives) > 0 and archives[0].is_incremental:
            raise BackupError('Oldest archive {} is incremental'
                              .format(archive.path()))
        self._total_size = 0
        self._count = 0
        prev = None
        for arch in archives:
            if prev:
                prev._next = arch
                arch._prev = prev
            else:
                self._first = arch
            self._total_size += arch.size
            self._count += 1
            prev = arch
        self._last = prev

    def __iter__(self):
        curr = self._first
        while curr:
            yield curr
            curr = curr._next

    def __len__(self):
        return self._count

    def remove(self, archive):
        if self._first == archive: self._first = archive._next
        if self._last == archive: self._last = archive._next
        if archive._prev: archive._prev._next = archive._next
        if archive._next: archive._next._prev = archive._prev
        archive._prev = archive._next = None
        self._total_size -= archive.size
        self._count -= 1

    def latest(self):
        return self._last

    def _archive_at_path(self, basedir, basename):
        if basename.startswith(self._name):
            m = _BASENAME_SUFFIX_RE.match(basename[len(self._name):])
            if m:
                return Archive(basedir, basename, m)
        return None
