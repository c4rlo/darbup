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
