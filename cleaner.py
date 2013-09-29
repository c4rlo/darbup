from errors import BackupError, NoRemovalCandidatesError

import logging, os, os.path

def make_cleaner(rmpolicy, arcset, now):
    def cleaner():
        arc = rmpolicy(arcset, now)
        if not arc:
            raise NoRemovalCandidatesError(rmpolicy, arcset)
        size = os.path.getsize(arc.path())
        logging.info('Removing {}, chosen by removal policy "{}", '
                     'to free up {} bytes'.format(arc.path(), rmpolicy.name,
                                                  size))
        os.remove(arc.path())
        arcset.remove(arc)
        return size
    return cleaner
