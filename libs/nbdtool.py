#!/usr/bin/env python

import os
import signal
import errno
import pickle
import subprocess
import time
from xapi.storage.libs import log

"""
Use "nbd-tool" to mirror disks between servers.
"""

persist_root = "/var/run/nonpersistent/persist-nbdtool/"
nbd_tool = "/usr/bin/nbd-tool"

def path_to_persist(mirror):
    return "%smirror.%d" % (persist_root,mirror.pid)

"""
ToDo: what is persist_foor?
def clear():
    call("clear", ["rm", "-rf", persist_foor])
"""


class Mirror:

    """An active nbd mirror"""

    def __init__(self, primary, secondary, pid, port):
        self.primary = primary
        self.secondary = secondary
        self.pid = pid
        self.port = port
        path = path_to_persist(self)
        to_create = os.path.dirname(path)
        try:
            os.makedirs(to_create)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(to_create):
                pass
            else:
                raise
        with open(path, 'w') as f:
            pickle.dump(self, f)

    def destroy(self, dbg):
        os.kill(self.pid, signal.SIGTERM)
        os.unlink(path_to_persist(self))


def find(dbg, primary, secondary):
    """Return the active mirror associated with the given primary and
       secondary"""
    used = set()
    try:
        used = set(os.listdir(persist_root))
    except OSError as exc:
        if exc.errno == errno.ENOENT:
            pass
        else:
            raise
    for filename in used:
        with open(persist_root + filename) as file:
            mirror = pickle.load(file)
            log.debug("file=%s" % file)
            if mirror.primary == primary and mirror.secondary == secondary:
                return mirror
    return None

def find_port(dbg):
    """Find an unused local port for nbd-tool to serve on"""
    used = set()
    
    try:
        used = set(os.listdir(persist_root))
    except OSError as exc:
        if exc.errno == errno.ENOENT:
            pass
        else:
            raise
    def getport(filename):
        with open(persist_root + filename) as file:
            m = pickle.load(file)
            return m.port
    myport = -1
    ports = [getport(f) for f in used]
    for port in range(10809,10909):
        if port not in ports:
            myport = port
            break
    
    return myport


def create(dbg, primary, secondary):
    """Return an active mirror associated with the given primary
       and secondary, creating a fresh one if one doesn't already exist."""
    existing = find(dbg, primary, secondary)
    if existing:
        return existing

    port = find_port(dbg)

    args = [nbd_tool, "mirror", primary, secondary, "--port", "%d" % port]
    log.debug(args)
    proc = subprocess.Popen(args)
    time.sleep(1)
    return Mirror(primary,secondary,proc.pid,port)

    """
    TODO: spawn nbd-client
    try:
        used = set(os.listdir(persist_root))
    except OSError as exc:
        if exc.errno == errno.ENOENT:
            pass
        else:
            raise
    """
    raise "Unimplemented"
