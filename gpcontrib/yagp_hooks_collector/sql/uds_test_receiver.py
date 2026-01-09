#!/usr/bin/env python3
import socket
import sys
import os
import time

sock_path, out_file, max_runtime = sys.argv[1], sys.argv[2], float(sys.argv[3])
deadline = time.time() + max_runtime

if os.path.exists(sock_path):
    os.unlink(sock_path)

count, total_bytes = 0, 0

with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
    s.bind(sock_path)
    s.listen(5)

    while (remaining := deadline - time.time()) > 0:
        s.settimeout(remaining)
        try:
            conn, _ = s.accept()
            with conn:
                conn.settimeout(1.0)
                total_bytes += len(conn.recv(2048))
                count += 1
        except socket.timeout:
            continue
        except OSError:
            break

if os.path.exists(sock_path):
    os.unlink(sock_path)

with open(out_file, 'w') as f:
    f.write(f"connections={count}\nreceived={str(total_bytes > 0).lower()}\n")
