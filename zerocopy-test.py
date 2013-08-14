#!/usr/bin/python3

import os, sys

BLOCK_SZ = (1 << 20)

if len(sys.argv) != 2:
    sys.stderr.write("usage: {} OUTFILE\n".format(sys.argv[0]))
    sys.exit(2)

out_name = sys.argv[1]

out_fd = os.open(out_name, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)

total = 0
count = 0

while True:
    rc = os.sendfile(out_fd, sys.stdin.fileno(), None, BLOCK_SZ)
    total += rc
    count += 1
    if rc < BLOCK_SZ: break

print("Wrote {} bytes in {} sendfile calls".format(total, count))
