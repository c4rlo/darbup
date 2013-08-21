import configparser, os.path, re, shlex

import schedules, rmpolicies
from errors import BackupError

class Instance:
    def __init__(self, name, dest_dir, dar_args, capacity, full_intvl,
                 incr_intvl, rm_policy):
        self.name = name
        self.dest_dir = dest_dir
        self.dar_args = dar_args
        self.capacity = capacity
        self.full_intvl = full_intvl
        self.incr_intvl = incr_intvl
        self.rm_policy = rm_policy

class Config:
    def __init__(self):
        parser = configparser.ConfigParser()
        parser['DEFAULT'] = {
            'FullBackupsInterval': 'monthly',
            'IncrBackupsInterval': 'daily',
            'RemovalPolicy': 'fifo'
        }
        parser.read((
            os.path.join(os.environ['HOME'], '.regudar', 'config'),
            '/etc/regudar.conf'
        ))

        self.instances = []

        for name, section in parser:
            self.instances.append(Instance(
                name,
                self._required_value(section, name, 'DestinationDir'),
                self._get_dar_args(section, name),
                self._get_capacity_value(section, name),
                schedules.schedule_by_name(section['FullBackupsInterval']),
                schedules.schedule_by_name(section['IncrBackupsInterval']),
                rmpolicies.rmpolicy_by_name(section['RemovalPolicy'])))

    def _required_value(self, section, section_name, name):
        if name not in section:
            raise BackupError('Configuration file section {} is missing '
                              'required setting "{}"'.format(section_name,
                                                             name))
        return section[name]

    def _get_dar_args(self, section, section_name):
        s = self._required_value(section, section_name, 'DarArguments')
        return shlex.split(s)

    CAPA_RE = re.compile(r'[0-9]+[kmgtp]$')
    CAPA_SUFFIX_FACTORS = {
        'k': (1 << 10),
        'm': (1 << 20),
        'g': (1 << 30),
        't': (1 << 40),
        'p': (1 << 50)
    }

    def _get_capacity_value(self, section, section_name):
        s = self._required_value(section, section_name, 'Capacity')
        m = self.CAPA_RE.match(s, re.IGNORECASE)
        if not m:
            raise BackupError('Configuration file section {} has bad Capacity '
                              'value "{}": must match /^{}/'.format(
                                  section_name, s, self.CAPA_RE.pattern))
        return int(s[:-1]) * self.CAPA_SUFFIX_FACTORS[s[-1]]
