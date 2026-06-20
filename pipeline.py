#!/usr/bin/env python3
"""Simple multi-process data pipeline using TCP sockets.

Usage:
  python3 pipeline.py --num 3 --topology ring --duration 10

Each worker runs a small TCP server and connects to peers defined by the topology.
They exchange newline-delimited JSON messages representing "fake work".
"""
import argparse
import json
import socket
import threading
import time
import traceback
from multiprocessing import Process, Event


def make_topology(num, kind="ring"):
    base_port = 50000
    nodes = [("127.0.0.1", base_port + i) for i in range(num)]
    peers = {i: [] for i in range(num)}
    if kind == "ring":
        for i in range(num):
            peers[i].append(((i + 1) % num))
    elif kind == "star":
        center = 0
        for i in range(1, num):
            peers[center].append(i)
    elif kind == "all":
        for i in range(num):
            peers[i] = [j for j in range(num) if j != i]
    else:
        raise ValueError("unknown topology: " + kind)
    # convert indices to (host,port)
    peers_addr = {
        i: [nodes[j] for j in peers[i]] for i in range(num)
    }
    return nodes, peers_addr


def server_thread(listen_host, listen_port, stop_event, proc_id):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((listen_host, listen_port))
    sock.listen(8)
    sock.settimeout(1.0)
    print(f"[P{proc_id}] Server listening on {listen_host}:{listen_port}")
    while not stop_event.is_set():
        try:
            conn, addr = sock.accept()
            t = threading.Thread(target=handle_conn, args=(conn, addr, stop_event, proc_id), daemon=True)
            t.start()
        except socket.timeout:
            continue
        except Exception:
            traceback.print_exc()
            break
    sock.close()


def handle_conn(conn, addr, stop_event, proc_id):
    with conn:
        f = conn.makefile("r")
        for line in f:
            if stop_event.is_set():
                break
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except Exception:
                print(f"[P{proc_id}] Received non-json: {line}")
                continue
            print(f"[P{proc_id}] RX from P{msg.get('src')}: seq={msg.get('seq')} payload={msg.get('payload')}")


def client_sender(proc_id, peers, stop_event, msgs_per_peer, interval):
    # For each peer, make a persistent connection and send messages periodically
    conns = {}
    seq = 0
    while not stop_event.is_set():
        for peer_idx, (host, port) in enumerate(peers):
            if stop_event.is_set():
                break
            key = f"{host}:{port}"
            if key not in conns:
                try:
                    s = socket.create_connection((host, port), timeout=3)
                    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    conns[key] = s
                    print(f"[P{proc_id}] Connected to {host}:{port}")
                except Exception:
                    # retry later
                    #print(f"[P{proc_id}] connect failed to {host}:{port}")
                    continue
            s = conns.get(key)
            if not s:
                continue
            try:
                for _ in range(msgs_per_peer):
                    if stop_event.is_set():
                        break
                    seq += 1
                    payload = {"src": proc_id, "dst": f"{host}:{port}", "seq": seq, "payload": f"work-{seq}", "ts": time.time()}
                    data = json.dumps(payload) + "\n"
                    s.sendall(data.encode("utf-8"))
                    print(f"[P{proc_id}] TX to {host}:{port} seq={seq}")
                    time.sleep(interval)
            except Exception:
                # drop connection and retry
                try:
                    s.close()
                except Exception:
                    pass
                if key in conns:
                    del conns[key]
        time.sleep(0.1)
    # close all
    for s in conns.values():
        try:
            s.close()
        except Exception:
            pass


def worker_main(proc_id, addr, peers, stop_event, msgs_per_peer, interval):
    host, port = addr
    # Start server thread
    st = threading.Thread(target=server_thread, args=(host, port, stop_event, proc_id), daemon=True)
    st.start()
    # wait a moment to let servers start across processes
    time.sleep(0.5)
    # Start client sender
    try:
        client_sender(proc_id, peers, stop_event, msgs_per_peer, interval)
    except KeyboardInterrupt:
        pass


def spawn_processes(num, topology, msgs_per_peer, send_interval, duration):
    nodes, peers_addr = make_topology(num, topology)
    stop_event = Event()
    procs = []
    for i, addr in enumerate(nodes):
        # pass peer list as list of (host,port)
        p = Process(target=worker_main, args=(i, addr, peers_addr[i], stop_event, msgs_per_peer, send_interval))
        p.start()
        procs.append(p)
        time.sleep(0.05)
    print("All workers started. Running for", duration, "seconds")
    try:
        t0 = time.time()
        while time.time() - t0 < duration:
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("Interrupted, shutting down")
    stop_event.set()
    for p in procs:
        p.join(timeout=2.0)


def main():
    parser = argparse.ArgumentParser(description="Multi-process pipeline demo using TCP sockets")
    parser.add_argument("--num", type=int, default=3, help="Number of worker processes")
    parser.add_argument("--topology", choices=("ring", "star", "all"), default="ring")
    parser.add_argument("--duration", type=int, default=10, help="Seconds to run")
    parser.add_argument("--msgs", type=int, default=1, help="Messages per peer per send loop")
    parser.add_argument("--interval", type=float, default=0.2, help="Seconds between messages when sending")
    args = parser.parse_args()

    spawn_processes(args.num, args.topology, args.msgs, args.interval, args.duration)


if __name__ == "__main__":
    main()
