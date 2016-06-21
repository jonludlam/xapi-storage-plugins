How to use the mirror APIs:

1. Install XenServer
2. Clone github.com:jonludlam/xapi-storage#sxm2-with-generated-code, copy python directory to XenServer and
   run 'python setup.py install'
3. Install 'data.py' onto the sender, and symlink to 'Data.mirror', 'Data.ls', 'Data.cancel', 'Data.stat'
3. Use github.com:jonludlam/smapiv3-dev and use this to build
   - xapi-nbd from github.com:xapi-project/xapi-nbd
   - nbdtool from github.com:xapi-project/nbd
4. Install nbdtool into the XenServer to send the image, xapi-nbd into the XenServer to receive the image
5. Open the firewall on the receiver (iptables -F will do it), and run xapi-nbd
6. Install the nbd module on the sender (modprobe nbd)
7. Install nbd-client from epel on the sender
8. Create a VDI on the sender (using GFS or FFS SR) (VDI1)
9. Create a VDI on the receiver (same size) (VDI2)
10. Attach the VDI on the source (e.g. /opt/xensource/debug/with-vdi $VDI1)
11. Write stuff into it
12. Start the mirror:
    ./Data.mirror dbg vhd+file:///path-to-local-vhd nbd:///root:\<password\>@\<destination host\>/\<VDI2\>
13. List the mirrors:
    ./Data.ls dbg
14. Cancel the miirror:
    ./Data.cancel dbg "[\"Mirror\",[\"vhd+file:.....\",\"...\"]]"

