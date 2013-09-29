import configparser, os.path, re, shlex

import schedules, rmpolicies
from errors import BackupError

class Config:
    def __init__(self, filename):
        parser = configparser.ConfigParser()
        parser['DEFAULT'] = {
            'FullBackupsInterval': 'monthly',
            'IncrBackupsInterval': 'daily',
            'RemovalPolicy': 'fifo',
            'LogsBackupCount': 60
        }

        parser.read(filename)

        self.instances = []

        for name, section in parser.items():
            if name == 'DEFAULT': continue
            cfg = _ConfigInstance()
            cfg.name = name
            cfg.dest_dir = self._required_value(section, name, 'DestinationDir')
            cfg.dar_args = self._get_dar_args(section, name)
            cfg.capacity = self._get_capacity_value(section, name)
            cfg.full_intvl = schedules.schedule_by_name(
                                section['FullBackupsInterval'])
            cfg.incr_intvl = schedules.schedule_by_name(
                                section['IncrBackupsInterval'])
            cfg.rmpolicy = rmpolicies.rmpolicy_by_name(section['RemovalPolicy'])
            cfg.logfilename = section.get('LogfileName')
            cfg.logsbackupcount = int(section.get('LogsBackupCount'))
            self.instances.append(cfg)

    def _required_value(self, section, section_name, name):
        if name not in section:
            raise BackupError('Configuration file section {} is missing '
                              'required setting "{}"'.format(section_name,
                                                             name))
        return section[name]

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
            raise BackupError('Configuration file section {} has bad Capacity '
                              'value "{}": must match /^{}/'.format(
                                  section_name, s, self.CAPA_RE.pattern))
        return int(s[:-1]) * self.CAPA_SUFFIX_FACTORS[s[-1].lower()]

class _ConfigInstance: pass
