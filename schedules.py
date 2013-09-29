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
