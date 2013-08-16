import re, os, os.path
from datetime import datetime
from operator import attrgetter


ARCHIVE_BASENAME_SUFFIX_PAT = \
        r'-(\d\d\d\d)-(\d\d)-(\d\d)-(\d\d)(\d\d)-(full|incr)\.1\.dar$'


class Archive:
    BASENAME_SUFFIX_RE = re.compile(ARCHIVE_BASENAME_SUFFIX_PAT)

    def __init__(self, basedir, basename, match):
        self.basedir = basedir
        self.basename = basename
        self.timestamp = datetime(int(match.group(1)), int(match.group(2)),
                                  int(match.group(3)), int(match.group(4)),
                                  int(match.group(5)))
        self.difftype = match.group(6)
        self.size = os.path.getsize(self.path())

    def path(self):
        return os.path.join(self.basedir, self.basename)


class ArchiveLister:
    def __init__(self, name):
        self.name = name

    def get_archives(self, path):
        archives = [ ]
        for fn in os.listdir(path):
            archive = self._archive_at_path(path, fn)
            if archive:
                archives.append(archive)
        archives.sort(key=attrgetter('timestamp'))
        previous = None
        for archive in archives:
            if archive.difftype == 'incr':
                if not previous:
                    raise BackupException('Oldest archive {} is '
                                          'incremental'.format(archive.path()))
            archive.reference = previous
            previous = archive
        return archives

    def _archive_at_path(self, basedir, basename):
        if basename.startswith(self.name):
            m = BASENAME_SUFFIX_RE.match(basename[len(self.name):])
            if m:
                return Archive(basedir, basename, m)
        return None
