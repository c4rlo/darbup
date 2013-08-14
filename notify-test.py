#!/usr/bin/python3

import os
import dbus

os.setgid(1000)
os.setuid(1000)

bus = dbus.SessionBus()
notif_obj = bus.get_object('org.freedesktop.Notifications',
                           '/org/freedesktop/Notifications')
notif_obj.Notify('Backup', 0, '', 'Something went wrong!',
                 'Total mess right here', [], {}, 0,
                 dbus_interface='org.freedesktop.Notifications')
