#!/usr/bin/env python

import urlparse
import os
import sys
import xapi
import xapi.storage.api.datapath
import xapi.storage.api.volume
from xapi.storage.libs import tapdisk, image, nbdtool, nbdclient
from xapi.storage import log
import pickle
import tempfile

MIRROR_METADATA_DIR = "/var/run/nonpersistent/dp-mirror/"

class Operation:

    def __init__(self, ty, src, dst, filename=None, nbddev=None):
        if ty not in ["Copy", "Mirror"]:
            raise xapi.InternalError("Operation type unknown (%s)" % ty)
        self.ty=ty
        self.src=src
        self.dst=dst
        self.filename=filename
        self.nbddev=nbddev

    def save(self):
        try:
            os.makedirs(MIRROR_METADATA_DIR, mode=0755)
        except OSError as e:
            if e.errno != 17: # EEXIST
                raise e

        if not self.filename:
            f=tempfile.NamedTemporaryFile(prefix='mirror', dir=MIRROR_METADATA_DIR, delete=False)
        else:
            f=open(self.filename,'w')
            f.truncate()

        try:
            self.filename=os.path.basename(f.name)
            pickle.dump(self.__dict__, f)
        finally:
            f.close()

    @staticmethod
    def load_all():
        all_files = set(os.listdir(MIRROR_METADATA_DIR))

        def load(filename):
            with open(MIRROR_METADATA_DIR+filename) as file:
                d = pickle.load(file)
                return Operation(d['ty'], d['src'], d['dst'], filename=filename, nbddev=d['nbddev'])
            
        return [load(d) for d in all_files]
        
    @staticmethod
    def find(ty, src, dst):
        all_ops = Operation.load_all()
        for op in all_ops:
            if op.ty==ty and op.src==src and op.dst==dst:
                return op

    @staticmethod
    def find_from_op(op):
        return Operation.find(op[0], op[1][0], op[1][1])
            
    def __repr__(self):
        return "Operation(%s, %s, %s)" % (self.ty, self.src, self.dst)

    def destroy(self):
        os.unlink(self.filename)
    
    def smapi_result(self):
        return [self.ty, [self.src, self.dst]]

def start_mirror(dbg, uri, domain, remote):
    u = urlparse.urlparse(uri)

    if not(os.path.exists(u.path)):
        raise xapi.storage.api.volume.Volume_does_not_exist(u.path)

    tap = tapdisk.load_tapdisk_metadata(dbg, u.path)
    tap.pause(dbg)
    
    # Create a new tapdisk that's talking to the original file:
    tap2 = tapdisk.create(dbg)
    tap2.open(dbg, tap.f, False)
    tapdisk.save_tapdisk_metadata(dbg, u.path, tap2)
    
    # Start nbd-tool to mirror. Primary is the path to the newly-created tapdisk's
    # block device, secondary is the uri we were given:
    nbdt = nbdtool.create(dbg, "file://%s" % tap2.block_device(), remote)

    # Create a new nbd client
    nbdc = nbdclient.create(dbg,"localhost","noname","%d" % nbdt.port)
    dev = "/dev/"+nbdc.nbd

    # Unpause the original tapdisk onto the nbd block device
    tap.unpause(dbg, image.Raw(dev))
    
    tapdisk.save_tapdisk_metadata(dbg, dev, tap)

    # If there's already an op (from a previously cancelled/completed mirror)
    # reuse that
    op = Operation.find("Mirror", uri, remote)
    if op:
        op.nbddev = nbdc.nbd
    else:
        op = Operation("Mirror",uri,remote,nbddev=nbdc.nbd)

    op.save()

    return op

def stop_mirror(dbg,mop):
    nbddev = "/dev/" + mop.nbddev
    tap = tapdisk.load_tapdisk_metadata(dbg, nbddev)
    # This is the tapdisk that's talking to the VM

    u = urlparse.urlparse(mop.src)
    tap2 = tapdisk.load_tapdisk_metadata(dbg, u.path)
    # This is the tapdisk that's still talking to the VHD

    # Pause the VM's tapdisk first:
    tap.pause(dbg)

    # Now we unmount the nbd device
    nbdc = nbdclient.find_by_device(dbg,mop.nbddev)
    nbdc.destroy(dbg)

    # Kill the nbd-tool process
    nbdt = nbdtool.find(dbg, "file://%s" % tap2.block_device(), mop.dst)
    nbdt.destroy (dbg)

    # Shut down the new tapdisk
    image = tap2.f
    tap2.close(dbg)
    tap2.destroy(dbg)

    # Unpause the VM's tapdisk
    tap.unpause(dbg,image)

    tapdisk.save_tapdisk_metadata(dbg, u.path, tap)
    tapdisk.forget_tapdisk_metadata(dbg, nbddev)
    


class Implementation(xapi.storage.api.datapath.Data_skeleton):
    def copy(self, dbg, uri, domain, remote):
        op = Operation("Copy",uri,remote)
        op.save()
        return op.smapi_result()
        
    def mirror(self, dbg, uri, domain, remote):
        op = start_mirror(dbg, uri, domain, remote)            
        op.save()
        return op.smapi_result()
    
    def ls(self, dbg):
        ops = Operation.load_all()
        return [op.smapi_result() for op in ops]

    def cancel(self, dbg, op):
        mop = Operation.find_from_op(op)

        if(mop.ty == "Mirror"):
            stop_mirror(dbg,mop)

        return ()

    def stat(self, dbg, op):
        op = Operation.find_from_op(op)
        if op:
            result = {}
            result['failed']=False
            result['progress']=0.5
        else:
            raise xapi.storage.api.volume.Volume_does_not_exist("")            
        return result
        
if __name__ == "__main__":
    log.log_call_argv()
    cmd = xapi.storage.api.datapath.Data_commandline(Implementation())
    base = os.path.basename(sys.argv[0])
    if base == "Data.mirror":
        cmd.mirror()
    elif base == "Data.copy":
        cmd.copy()
    elif base == "Data.stat":
        cmd.stat()
    elif base == "Data.cancel":
        cmd.cancel()
    elif base == "Data.destroy":
        cmd.detroy()
    elif base == "Data.ls":
        cmd.ls()
    else:
        raise xapi.storage.api.datapath.Unimplemented(base)
