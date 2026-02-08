#!/usr/bin/env python

"""Switch Code for ECE50863 Lab Project 1
Author: Matt Bowring
Email: mbowring@purdue.edu
"""

import sys
import socket
import struct
import threading
import time
from datetime import datetime
from typing import Dict, List, Any, Optional

from common import (
    RoutingEntry, NeighborInfo,
    LOCALHOST, BUFFER_SIZE, UPDATE_DELAY, TIMEOUT,
    KEY_NEIGHBOR_ID, KEY_ALIVE, KEY_HOST, KEY_PORT,
    BIN_REGISTER_RESPONSE, BIN_ROUTING_UPDATE, BIN_KEEP_ALIVE,
    serialize_register_request, deserialize_register_response, deserialize_routing_update,
    serialize_keep_alive, deserialize_keep_alive, serialize_topology_update,
)

# Please do not modify the name of the log file, otherwise you will lose points because the grader won't be able to find your log file
LOG_FILE = "switch#.log" # The log file for switches are switch#.log, where # is the id of that switch (i.e. switch0.log, switch1.log). The code for replacing # with a real number has been given to you in the main function.

# Those are logging functions to help you follow the correct logging standard

# "Register Request" Format is below:
#
# Timestamp
# Register Request Sent

def register_request_sent() -> None:
    log: List[str] = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Register Request Sent\n")
    write_to_log(log)

# "Register Response" Format is below:
#
# Timestamp
# Register Response Received

def register_response_received() -> None:
    log: List[str] = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Register Response Received\n")
    write_to_log(log)

# For the parameter "routing_table", it should be a list of lists in the form of [[...], [...], ...].
# Within each list in the outermost list, the first element is <Switch ID>. The second is <Dest ID>, and the third is <Next Hop>.
# "Routing Update" Format is below:
#
# Timestamp
# Routing Update
# <Switch ID>,<Dest ID>:<Next Hop>
# ...
# ...
# Routing Complete
#
# You should also include all of the Self routes in your routing_table argument -- e.g.,  Switch (ID = 4) should include the following entry:
# 4,4:4

def routing_table_update(routing_table: List[RoutingEntry]) -> None:
    log: List[str] = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append("Routing Update\n")
    for row in routing_table:
        log.append(f"{row[0]},{row[1]}:{row[2]}\n")
    log.append("Routing Complete\n")
    write_to_log(log)

# "Unresponsive/Dead Neighbor Detected" Format is below:
#
# Timestamp
# Neighbor Dead <Neighbor ID>

def neighbor_dead(switch_id: int) -> None:
    log: List[str] = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Neighbor Dead {switch_id}\n")
    write_to_log(log)

# "Unresponsive/Dead Neighbor comes back online" Format is below:
#
# Timestamp
# Neighbor Alive <Neighbor ID>

def neighbor_alive(switch_id: int) -> None:
    log: List[str] = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Neighbor Alive {switch_id}\n")
    write_to_log(log)

def write_to_log(log: List[str]) -> None:
    with open(LOG_FILE, 'a+') as log_file:
        log_file.write("\n\n")
        # Write to log
        log_file.writelines(log)

def register_with_controller(sid: int, host: str, port: int) -> Optional[List[NeighborInfo]]:
    # Create a UDP socket for communication with controller and other switches
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Bind to localhost
    sock.bind((LOCALHOST, 0))
    sport = sock.getsockname()[1]

    # Send Register Request to controller via UDP (binary format)
    sock.sendto(
        serialize_register_request(sid, sport),
        (host, port)
    )
    register_request_sent()

    # Receive Register Response from controller (binary format)
    data, _ = sock.recvfrom(BUFFER_SIZE)
    msg_type = struct.unpack('!B', data[:1])[0]

    if msg_type == BIN_REGISTER_RESPONSE:
        register_response_received()
        nbrs = deserialize_register_response(data)
        return sock, nbrs

    return None

def main() -> None:
    global LOG_FILE

    # Check for number of arguments and exit if host/port not provided
    if len(sys.argv) < 4:
        print("switch.py <Id_self> <Controller hostname> <Controller Port>\n")
        sys.exit(1)

    sid: int = int(sys.argv[1])
    host: str = sys.argv[2]
    port: int = int(sys.argv[3])

    LOG_FILE = 'switch' + str(sid) + ".log"

    # Register with controller and get neighbor information
    result = register_with_controller(sid, host, port)
    if result is None:
        sys.exit(1)

    sock, nbrs = result

    # Receive routing update (binary format)
    data, _ = sock.recvfrom(BUFFER_SIZE)
    msg_type = struct.unpack('!B', data[:1])[0]
    if msg_type == BIN_ROUTING_UPDATE:
        routes = deserialize_routing_update(data)
        routing_table_update(routes)

    # Parse -f flag for link failure simulation
    failed_neighbor: Optional[int] = None
    if len(sys.argv) >= 6 and sys.argv[4] == '-f':
        failed_neighbor = int(sys.argv[5])

    # Initialize neighbor state from register response
    lock = threading.Lock()
    neighbors: Dict[int, Dict[str, Any]] = {}
    for nbr in nbrs:
        neighbors[nbr[KEY_NEIGHBOR_ID]] = {
            KEY_HOST: nbr[KEY_HOST],
            KEY_PORT: nbr[KEY_PORT],
            KEY_ALIVE: True,
            'last_heard': time.time()
        }
    controller_addr = (host, port)

    def send_topology_update() -> None:
        nbr_list = [(nid, info[KEY_ALIVE]) for nid, info in neighbors.items()]
        sock.sendto(
            serialize_topology_update(sid, nbr_list),
            controller_addr
        )

    def periodic_tasks() -> None:
        while True:
            time.sleep(UPDATE_DELAY)
            with lock:
                now = time.time()

                # Check for timed-out neighbors
                for nid, info in neighbors.items():
                    if info[KEY_ALIVE] and (now - info['last_heard']) >= TIMEOUT:
                        info[KEY_ALIVE] = False
                        neighbor_dead(nid)

                # Send KEEP_ALIVE to each alive neighbor (skip failed)
                for nid, info in neighbors.items():
                    if not info[KEY_ALIVE]:
                        continue
                    if failed_neighbor is not None and nid == failed_neighbor:
                        continue
                    sock.sendto(
                        serialize_keep_alive(sid),
                        (info[KEY_HOST], info[KEY_PORT])
                    )

                # Send Topology Update to controller
                send_topology_update()

    # Start periodic timer thread
    timer = threading.Thread(target=periodic_tasks, daemon=True)
    timer.start()

    # Main thread: recv loop
    while True:
        data, addr = sock.recvfrom(BUFFER_SIZE)
        msg_type = struct.unpack('!B', data[:1])[0]

        if msg_type == BIN_KEEP_ALIVE:
            sender_id = deserialize_keep_alive(data)

            # Ignore keep-alive from failed neighbor
            if failed_neighbor is not None and sender_id == failed_neighbor:
                continue

            with lock:
                if sender_id in neighbors:
                    was_dead = not neighbors[sender_id][KEY_ALIVE]
                    neighbors[sender_id]['last_heard'] = time.time()

                    if was_dead:
                        neighbors[sender_id][KEY_ALIVE] = True
                        neighbors[sender_id][KEY_HOST] = addr[0]
                        neighbors[sender_id][KEY_PORT] = addr[1]
                        neighbor_alive(sender_id)
                        send_topology_update()

        elif msg_type == BIN_ROUTING_UPDATE:
            routes = deserialize_routing_update(data)
            routing_table_update(routes)

if __name__ == "__main__":
    main()
