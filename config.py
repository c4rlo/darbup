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

import sys, configparser, os, os.path, re, shlex

import schedules, rmpolicies
from errors import BackupError

class Config:
    def __init__(self, filename):
        if not os.path.exists(filename):
            raise BackupError('Configuration file {} does not exist'
                              .format(filename))

        parser = configparser.ConfigParser()
        parser['DEFAULT'] = {
            'FullBackupsInterval': 'monthly',
            'IncrBackupsInterval': 'daily',
            'RemovalPolicy': 'thinning',
            'IgnoreChangingFiles': False,
            'LogsBackupCount': 60
        }

        parser.read(filename)

        self.instances = []

        for section_name, section in parser.items():
            if section_name == 'DEFAULT': continue
            if not section_name.lower().startswith('backup '):
                raise BackupError('Configuration file section name "{}" is '
                                  'invalid: must begin with "Backup "'
                                  .format(section_name))
            cfg = _ConfigInstance()
            cfg.name = section_name[7:]
            cfg.dest_dir = self._required_value(section, section_name,
                                                'DestinationDir')
            cfg.dar_args = self._get_dar_args(section, section_name)
            cfg.capacity = self._get_capacity_value(section, section_name)
            cfg.full_intvl = schedules.schedule_by_name(
                                section['FullBackupsInterval'])
            cfg.incr_intvl = schedules.schedule_by_name(
                                section['IncrBackupsInterval'])
            cfg.rmpolicy = rmpolicies.rmpolicy_by_name(section['RemovalPolicy'])
            cfg.ignore_changing_files = self._bool_value(section, section_name,
                                'IgnoreChangingFiles')
            cfg.logfilename = section.get('LogfileName')
            cfg.logsbackupcount = int(section.get('LogsBackupCount'))
            self.instances.append(cfg)

    def _required_value(self, section, section_name, name):
        if name not in section:
            raise BackupError('Configuration file section "{}" is missing '
                              'required setting "{}"'.format(section_name,
                                                             name))
        return section[name]

    def _bool_value(self, section, section_name, name):
        value = section[name]
        if isinstance(value, bool): return value
        v = value.lower()
        if v in ('1', 'yes', 'true', 'on'):
            return True
        elif v in ('0', 'no', 'false', 'off'):
            return False
        else:
            raise BackupError('Configuration file section "{}" setting "{}" '
                              'is not a valid boolean value'.format(
                                  section_name, name))

    def _get_dar_args(self, section, section_name):
        s = self._required_value(section, section_name, 'DarArguments')
        return shlex.split(s)

    CAPA_RE = re.compile(r'[0-9]+[kmgtp]$', re.IGNORECASE)
    CAPA_SUFFIX_FACTORS = {
        'k': (1 << 10),
        'm': (1 << 20),
        'g': (1 << 30),
        't': (1 << 40),
        'p': (1 << 50)
    }

    def _get_capacity_value(self, section, section_name):
        s = self._required_value(section, section_name, 'Capacity')
        m = self.CAPA_RE.match(s)
        if not m:
            raise BackupError('Configuration file section "{}" has bad '
                              'Capacity value "{}": must match /^{}/'.format(
                                  section_name, s, self.CAPA_RE.pattern))
        return int(s[:-1]) * self.CAPA_SUFFIX_FACTORS[s[-1].lower()]

class _ConfigInstance: pass

_DEFAULT_CONFIG = b'''\
# darbup configuration file
#
# General format: .ini file style.
# One section per set of data you wish to archive.
#
# Example section (remove '#' to uncomment):
#
# [Backup stuff]
# # Section names must begin with "Backup ". The arbitrary identifier that
# # follows determines the basename of generate archives.
#
# DarArguments=-R /home/fred
# # Arguments passed to /usr/bin/dar. Do NOT include -c or -A, as darbup adds
# # those arguments automatically as required. Typically, you just want a -R to
# # specify the directory you want to archive, perhaps with some -I/-X/-P/-g
# # for further refinement. Compression options like -z may also be useful.
#
# DestinationDir=/backup
# # Directory where to place the generated archive files
#
# Capacity=500G
# # Maximum amount of space to use for archives. Valid suffixes are K, M, G, T,
# # P, for KiBi-, MeBi-, GiBi-, TeBi-, PeBi-bytes, respectively.
# # Note 1: only archives generated by darbup are taken into account when
# # calculating used space.
# # Note 2: currently, there is no check for whether the disk is full, so make
# # sure this value does not exceed the actual free space (otherwise darbup will
# # fail with a nasty error when the disk is full).
#
# FullBackupsInterval=monthly
# # Frequency with which to generate full (non-incremental) backups. Valid
# # values:
# # - monthly: creates a new full backup if the calendar month has changed
# #   since the last time a full backup was made
# # - daily: creates a new full backup if the day (00:00-23:59 period) has
# #   changed since the time a full backup was made
# # - always: creates a new full backup on every invocation of darbup
#
# IncrBackupsInterval=daily
# # Frequency with which to generate incremental backups. These are incremental
# # relative to the previous backup (which may itself be incremental). Valid
# # values are the same as for FullBackupsInterval.
#
# RemovalPolicy=thinning
# # When the disk is full -- as defined by the Capacity option -- we must delete
# # an old archive. This option determines how we pick it. Valid values:
# # - thinning: use a scoring method that prefers older archives, and those
# #   where another archive exists that was created around the same time
# # - oldest: delete the oldest archive
# # - never: do not delete any archive, but print an error and exit
# # Note: we never delete archives that serve as reference for other
# # (incremental) archives.
#
# IgnoreChangingFiles=false
# # If dar detects that files are changing while it is reading them, those files
# # within the archive may contain bad (incomplete) data. By default, we
# # consider this a failure, and do not create a backup. Set this option to
# # 'true' to create the backup archive anyway. The logs will contain messages
# # explaining what happened and what the affected files are.
# # See also the --retry-on-change option in "man dar"; this may be specified as
# # part of the DarArguments configuration setting (see above) to change dar's
# # behaviour (only sensible if IgnoreChangingFiles=true).
#
# LogfileName=/home/fred/.darbup/logs/stuff.log
# # Logfile name.
#
# LogsBackupCount=60
# # How many back copies of logs to retain.
'''

def write_default_config(filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'wb') as f:
        f.write(_DEFAULT_CONFIG)
    sys.stderr.write('Default configuration file written to {}, '
                     'please customize\n'.format(filename))
