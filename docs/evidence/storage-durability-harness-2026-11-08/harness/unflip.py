import sys, zlib
path, off, end = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
data = bytearray(open(path, "rb").read())
def valid(buf, o):
    h = buf[o:o+16]
    if h[:4] != b"BICF" or h[4] != 1: return False
    ln = int.from_bytes(h[8:12], "little")
    crc = int.from_bytes(h[12:16], "little")
    c = zlib.crc32(bytes([1, h[5]]) + bytes(h[6:8]) + bytes(h[8:12]) + bytes(buf[o+16:o+16+ln]))
    return c == crc and o + 16 + ln == end
for pos in range(off, end):
    data[pos] ^= 0xFF
    if valid(data, off):
        print("restored byte at", pos)
        open(path, "wb").write(data)
        sys.exit(0)
    data[pos] ^= 0xFF
print("not found"); sys.exit(1)
