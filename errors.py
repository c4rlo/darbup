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
