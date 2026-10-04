import os, sys, fcntl, struct
FS_IOC_FIEMAP = 0xC020660B
LO, HI = 2906949445*4096, 2906949479*4096   # 壞區塊的位元組範圍(ext4 4K)
N = 512
def extents(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        start = 0
        while True:
            hdr = struct.pack('=QQLLLL', start, 2**64-1, 0, 0, N, 0)
            buf = bytearray(hdr + b'\0'*(56*N))
            fcntl.ioctl(fd, FS_IOC_FIEMAP, buf)
            cnt = struct.unpack_from('=L', buf, 20)[0]
            if cnt == 0: return
            last = False
            for i in range(cnt):
                lg, ph, ln, _, _, fl = struct.unpack_from('=QQQQQL', buf, 32+56*i)
                yield lg, ph, ln
                if fl & 1: last = True
                start = lg + ln
            if last: return
    finally:
        os.close(fd)
hits, err = 0, 0
for root, dirs, files in os.walk('/srv/nas'):
    for f in files:
        p = os.path.join(root, f)
        try:
            for lg, ph, ln in extents(p):
                if ph < HI and ph + ln > LO:
                    print('HIT', p, 'logical_off', lg + max(0, LO-ph), 'size', os.path.getsize(p), flush=True); hits += 1
        except OSError:
            err += 1
print('done hits', hits, 'unreadable', err)
