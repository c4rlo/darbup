class BackupError(Exception):
    def __init__(self, msg):
        self._msg = msg

    def __str__(self):
        return self._msg

class NoRemovalCandidatesError(BackupError):
    def __init__(self, rmpolicy, arcset):
        self._msg = 'Removal policy "{}" failed to find a suitable archive ' \
                'to delete from existing set ({})'.format(rmpolicy.name, arcset)

    def __str__(self):
        return self._msg

class TerminatedSignal(Exception):
    def __str__(self):
        return 'Terminated'

def exc_str(exception):
    return str(exception) or type(exception).__name__
