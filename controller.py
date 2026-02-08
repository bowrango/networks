#!/usr/bin/env python

"""Controller Code for ECE50863 Lab Project 1
Author: Matt Bowring
Email: mbowring@purdue.edu
"""

import sys
import socket
import struct
import threading
import time
from datetime import datetime
import heapq
from typing import Dict, List, Tuple, Optional

from common import (
    Topology, SwitchInfo, RoutingEntry, NeighborInfo,
    LOCALHOST, BUFFER_SIZE, UNREACHABLE_DISTANCE, UNREACHABLE_HOP,
    KEY_HOST, KEY_PORT, KEY_NEIGHBOR_ID, KEY_ALIVE,
    BIN_REGISTER_REQUEST, BIN_TOPOLOGY_UPDATE,
    UPDATE_DELAY, TIMEOUT,
    serialize_register_response, serialize_routing_update,
    deserialize_register_request, deserialize_topology_update,
)

# Please do not modify the name of the log file, otherwise you will lose points because the grader won't be able to find your log file
LOG_FILE = "Controller.log"

# Those are logging functions to help you follow the correct logging standard

# "Register Request" Format is below:
#
# Timestamp
# Register Request <Switch-ID>

def register_request_received(switch_id: int) -> None:
    log: List[str] = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Register Request {switch_id}\n")
    write_to_log(log)

# "Register Response" Format is below (for every switch):
#
# Timestamp
# Register Response <Switch-ID>

def register_response_sent(switch_id: int) -> None:
    log: List[str] = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Register Response {switch_id}\n")
    write_to_log(log)

# For the parameter "routing_table", it should be a list of lists in the form of [[...], [...], ...].
# Within each list in the outermost list, the first element is <Switch ID>. The second is <Dest ID>, and the third is <Next Hop>, and the fourth is <Shortest distance>
# "Routing Update" Format is below:
#
# Timestamp
# Routing Update
# <Switch ID>,<Dest ID>:<Next Hop>,<Shortest distance>
# ...
# ...
# Routing Complete
#
# You should also include all of the Self routes in your routing_table argument -- e.g.,  Switch (ID = 4) should include the following entry:
# 4,4:4,0
# 0 indicates 'zero' distance
#
# For switches that can't be reached, the next hop and shortest distance should be '-1' and '9999' respectively. (9999 means infinite distance so that that switch can't be reached)
#  E.g, If switch=4 cannot reach switch=5, the following should be printed
#  4,5:-1,9999
#
# For any switch that has been killed, do not include the routes that are going out from that switch.
# One example can be found in the sample log in starter code.
# After switch 1 is killed, the routing update from the controller does not have routes from switch 1 to other switches.

def routing_table_update(routing_table: List[RoutingEntry]) -> None:
    log: List[str] = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append("Routing Update\n")
    for row in routing_table:
        log.append(f"{row[0]},{row[1]}:{row[2]},{row[3]}\n")
    log.append("Routing Complete\n")
    write_to_log(log)

# "Topology Update: Link Dead" Format is below: (Note: We do not require you to print out Link Alive log in this project)
#
#  Timestamp
#  Link Dead <Switch ID 1>,<Switch ID 2>

def topology_update_link_dead(switch_id_1: int, switch_id_2: int) -> None:
    log: List[str] = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Link Dead {switch_id_1},{switch_id_2}\n")
    write_to_log(log)

# "Topology Update: Switch Dead" Format is below:
#
#  Timestamp
#  Switch Dead <Switch ID>

def topology_update_switch_dead(switch_id: int) -> None:
    log: List[str] = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Switch Dead {switch_id}\n")
    write_to_log(log)

# "Topology Update: Switch Alive" Format is below:
#
#  Timestamp
#  Switch Alive <Switch ID>

def topology_update_switch_alive(switch_id: int) -> None:
    log: List[str] = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Switch Alive {switch_id}\n")
    write_to_log(log)

def write_to_log(log: List[str]) -> None:
    with open(LOG_FILE, 'a+') as log_file:
        log_file.write("\n\n")
        # Write to log
        log_file.writelines(log)

class RoutingCache:
    def __init__(self) -> None:
        self._last_topo: Optional[Topology] = None
        self.routes_by_switch: Dict[int, List[RoutingEntry]] = {}
        self._n: int = 0

    def update(self, topo: Topology, n: int) -> bool:
        if self._last_topo == topo and self._n == n:
            return False
        self.routes_by_switch = self._compute_routing_tables(topo, n)
        self._last_topo = topo
        self._n = n
        return True

    def flat_routes(self, switch_alive: Optional[Dict[int, bool]] = None) -> List[RoutingEntry]:
        return [r for sid, routes in self.routes_by_switch.items()
                if switch_alive is None or switch_alive.get(sid, False)
                for r in routes]

    def _compute_routing_tables(self, topo: Topology, n: int) -> Dict[int, List[RoutingEntry]]:
        by_switch: Dict[int, List[RoutingEntry]] = {}
        for sid in range(n):
            dist, hop = self._dijkstra(sid, topo, n)
            by_switch[sid] = []
            for did in range(n):
                if dist[did] == float('inf'):
                    by_switch[sid].append([sid, did, UNREACHABLE_HOP, UNREACHABLE_DISTANCE])
                else:
                    by_switch[sid].append([sid, did, hop[did], int(dist[did])])
        return by_switch

    def _dijkstra(self, src: int, topo: Topology, n: int) -> Tuple[Dict[int, float], Dict[int, int]]:
        # Compute shortest path from the source switch to all reachable switches
        dist = {i: float('inf') for i in range(n)}
        dist[src] = 0
        prev = {i: None for i in range(n)}
        vis = set()

        pq = [(0, src)]
        while pq:
            d, u = heapq.heappop(pq)
            if u in vis:
                continue

            vis.add(u)

            for v, cost in topo.get(u, []):
                alt = d + cost
                if alt < dist[v]:
                    dist[v] = alt
                    prev[v] = u
                    heapq.heappush(pq, (alt, v))

        # Build next hop table
        hop: Dict[int, int] = {}
        for dst in range(n):
            if dst == src:
                hop[dst] = src
            elif dist[dst] == float('inf'):
                hop[dst] = UNREACHABLE_HOP
            else:
                node = dst
                while prev[node] != src and prev[node] is not None:
                    node = prev[node]
                hop[dst] = node if prev[node] == src else UNREACHABLE_HOP

        return dist, hop

def build_neighbor_list(topo: Topology, sid: int, sw: Dict[int, SwitchInfo],
                        switch_alive: Optional[Dict[int, bool]] = None) -> List[NeighborInfo]:
    nbrs = []
    for nid, _ in topo.get(sid, []):
        nbrs.append({
            KEY_NEIGHBOR_ID: nid,
            KEY_ALIVE: switch_alive.get(nid, False) if switch_alive else True,
            KEY_HOST: sw.get(nid, {}).get(KEY_HOST, LOCALHOST),
            KEY_PORT: sw.get(nid, {}).get(KEY_PORT, 0)
        })
    return nbrs

def bootstrap(port: int, cfg: str) -> Tuple[socket.socket, Dict[int, SwitchInfo], Topology]:
    # Register Switches with the Controller

    # Parse the config file to get topology information
    topo: Topology = {}
    n: int = 0
    with open(cfg, 'r') as f:
        lines = f.readlines()
        n = int(lines[0].strip())

        # Initialize topology for all switches
        for i in range(n):
            topo[i] = []

        # Parse topology edges
        for line in lines[1:]:
            line = line.strip()
            if line:
                parts = line.split()
                s1 = int(parts[0])
                s2 = int(parts[1])
                dist = int(parts[2])
                # Bidirectional
                topo[s1].append((s2, dist))
                topo[s2].append((s1, dist))

    # Controller binds to a well-known port number
    ctrl = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ctrl.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ctrl.bind((LOCALHOST, port))

    # Store information about registered switches
    sw: Dict[int, SwitchInfo] = {}
    while len(sw) < n:
        # Receive Register Request from switch
        data, addr = ctrl.recvfrom(BUFFER_SIZE)
        msg_type = struct.unpack('!B', data[:1])[0]

        assert msg_type == BIN_REGISTER_REQUEST

        sid, sport = deserialize_register_request(data)

        # Log the Register Request
        register_request_received(sid)

        # Store switch information
        sw[sid] = {
            KEY_HOST: addr[0],
            KEY_PORT: sport
        }

    # Send Register Response to each switch once they've been registered
    for sid, info in sw.items():
        nbrs = build_neighbor_list(topo, sid, sw)
        ctrl.sendto(
            serialize_register_response(nbrs),
            (info[KEY_HOST], info[KEY_PORT])
        )
        register_response_sent(sid)

    return ctrl, sw, topo

def build_topology(topo_template: Topology, switch_alive: Dict[int, bool],
                   switch_neighbors: Dict[int, Dict[int, bool]]) -> Topology:
    current_topo: Topology = {}
    for sid in topo_template:
        if not switch_alive.get(sid, False):
            continue
        current_topo[sid] = []
        for nid, cost in topo_template[sid]:
            if not switch_alive.get(nid, False):
                continue
            a_sees_b = switch_neighbors.get(sid, {}).get(nid, True)
            b_sees_a = switch_neighbors.get(nid, {}).get(sid, True)
            if a_sees_b and b_sees_a:
                current_topo[sid].append((nid, cost))
    return current_topo

def send_routing_updates(ctrl: socket.socket, sw: Dict[int, SwitchInfo],
                         routes_by_switch: Dict[int, List[RoutingEntry]],
                         switch_alive: Optional[Dict[int, bool]] = None) -> None:
    # Send routing update to each switch (binary format)
    for sid, rt in routes_by_switch.items():
        if sid not in sw:
            continue
        if switch_alive is not None and not switch_alive.get(sid, False):
            continue
        ctrl.sendto(
            serialize_routing_update(rt),
            (sw[sid][KEY_HOST], sw[sid][KEY_PORT])
        )

def main() -> None:
    # Check for number of arguments and exit if host/port not provided
    num_args: int = len(sys.argv)
    if num_args < 3:
        print("Usage: python controller.py <port> <config file>\n")
        sys.exit(1)

    port = int(sys.argv[1])
    cfg = str(sys.argv[2])

    cache = RoutingCache()

    # Setup socket connection to switches
    ctrl, sw, topo = bootstrap(port, cfg)

    # Compute routing tables
    n = len(sw)
    cache.update(topo, n)

    # Log routing update
    routing_table_update(cache.flat_routes())

    # Send routing updates to all switches
    send_routing_updates(ctrl, sw, cache.routes_by_switch)

    # Initialize state for topology change tracking
    topo_template = topo
    lock = threading.Lock()
    switch_alive: Dict[int, bool] = {sid: True for sid in sw}
    last_heard: Dict[int, float] = {sid: time.time() for sid in sw}
    switch_neighbors: Dict[int, Dict[int, bool]] = {}
    for sid in sw:
        switch_neighbors[sid] = {nid: True for nid, _ in topo_template[sid]}

    def recompute_and_send() -> None:
        current_topo = build_topology(topo_template, switch_alive, switch_neighbors)
        if cache.update(current_topo, n):
            routing_table_update(cache.flat_routes(switch_alive))
            send_routing_updates(ctrl, sw, cache.routes_by_switch, switch_alive)

    def periodic_check() -> None:
        while True:
            time.sleep(UPDATE_DELAY)
            with lock:
                now = time.time()
                changed = False
                for sid in list(last_heard.keys()):
                    if switch_alive.get(sid, False) and (now - last_heard[sid]) >= TIMEOUT:
                        switch_alive[sid] = False
                        topology_update_switch_dead(sid)
                        changed = True
                if changed:
                    recompute_and_send()

    # Start periodic check thread
    checker = threading.Thread(target=periodic_check, daemon=True)
    checker.start()

    # Main thread: recv loop
    while True:
        data, addr = ctrl.recvfrom(BUFFER_SIZE)
        msg_type = struct.unpack('!B', data[:1])[0]

        if msg_type == BIN_TOPOLOGY_UPDATE:
            sender_id, nbr_status = deserialize_topology_update(data)

            with lock:
                last_heard[sender_id] = time.time()

                # Update switch address (handles port changes on restart)
                sw[sender_id][KEY_HOST] = addr[0]
                sw[sender_id][KEY_PORT] = addr[1]

                # Check if switch was previously dead
                if not switch_alive.get(sender_id, True):
                    switch_alive[sender_id] = True
                    topology_update_switch_alive(sender_id)

                # Detect link deaths
                old_nbrs = switch_neighbors.get(sender_id, {})
                for nid, alive in nbr_status:
                    was_alive = old_nbrs.get(nid, True)
                    if was_alive and not alive:
                        topology_update_link_dead(sender_id, nid)

                # Update neighbor status
                switch_neighbors[sender_id] = {nid: alive for nid, alive in nbr_status}

                recompute_and_send()

        elif msg_type == BIN_REGISTER_REQUEST:
            # Handle re-registration of a restarted switch
            sid_restart, sport_restart = deserialize_register_request(data)

            with lock:
                sw[sid_restart] = {KEY_HOST: addr[0], KEY_PORT: sport_restart}

                # Send register response with current neighbor info
                nbrs = build_neighbor_list(topo_template, sid_restart, sw, switch_alive)
                ctrl.sendto(
                    serialize_register_response(nbrs),
                    (addr[0], sport_restart)
                )
                register_request_received(sid_restart)
                register_response_sent(sid_restart)

                # Mark switch alive
                was_dead = not switch_alive.get(sid_restart, True)
                switch_alive[sid_restart] = True
                last_heard[sid_restart] = time.time()
                switch_neighbors[sid_restart] = {nid: True for nid, _ in topo_template.get(sid_restart, [])}

                if was_dead:
                    topology_update_switch_alive(sid_restart)

                recompute_and_send()

                # Send this switch its specific routes
                ctrl.sendto(
                    serialize_routing_update(cache.routes_by_switch.get(sid_restart, [])),
                    (addr[0], sport_restart)
                )

if __name__ == "__main__":
    main()
