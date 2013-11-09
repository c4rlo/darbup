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

from errors import BackupError

class Schedules:
    @staticmethod
    def monthly(prev, now):
        return prev.month != now.month or prev.year != now.year

    @staticmethod
    def daily(prev, now):
        return prev.day != now.day or prev.month != now.month \
                or prev.year != now.year

    @staticmethod
    def always(prev, now):
        return True

def schedule_by_name(name):
    sched = getattr(Schedules, name, None)
    if sched: return sched
    raise BackupError('Invalid backup schedule: "{}"'.format(name))
