from errors import BackupError

class FifoPolicy:
    def __call__(self, arcset, now):
        for arc in arcset:
            if not (arc.is_current or arc.has_dependent()):
                return arc

class ThinningPolicy:
    def __call__(self, arcset, now):
        best_arc = None
        best_score = float('inf')
        for arc in arcset:
            if arc.is_current: continue
            if not arc.prev(): continue
            if arc.has_dependent(): continue
            prev_time = arc.prev().timestamp
            if arc.next():
                next_time = arc.next().timestamp
            else:
                next_time = now
            score = _to_secs(arc.timestamp - prev_time) * \
                    _to_secs(next_time - arc.timestamp) / \
                    _to_secs(now - arc.timestamp)
            if score < best_score:
                best_score = score
                best_arc = arc
        if best_arc is None and len(arcset) > 0:
            first = arcset.first()
            if not first.has_dependent() and not first.is_current:
                return first
        return best_arc

def rmpolicy_by_name(name):
    pol = None
    if name == 'fifo': pol = FifoPolicy()
    elif name == 'thinning': pol = ThinningPolicy()
    else:
        raise BackupError('Invalid removal policy: "{}"'.format(name))
    pol.name = name
    return pol

def _to_secs(tdelta):
    return tdelta.days * 86400 + tdelta.seconds + \
            (1 if tdelta.microseconds >= 500000 else 0)
