"""Constants and utilities for Controller and Switches
Author: Matt Bowring
Email: mbowring@purdue.edu
"""

import struct
from typing import Dict, List, Tuple, Any

# Type aliases
Topology = Dict[int, List[Tuple[int, int]]]  # {switch_id: [(neighbor_id, cost), ...]}
SwitchInfo = Dict[str, Any]  # {'host': str, 'port': int}
RoutingEntry = List[int]  # [switch_id, dest_id, next_hop, distance]
NeighborInfo = Dict[str, Any]  # {'id': int, 'alive': bool, 'host': str, 'port': int}

# Network constants
LOCALHOST: str = '127.0.0.1'
BUFFER_SIZE: int = 4096
UPDATE_DELAY: int = 2  # (seconds)
TIMEOUT: int = 3 * UPDATE_DELAY

# Distance constants
UNREACHABLE_DISTANCE: int = 9999
UNREACHABLE_HOP: int = -1

# Message keys
KEY_PORT: str = 'port'
KEY_NEIGHBOR_ID: str = 'id'
KEY_ALIVE: str = 'alive'
KEY_HOST: str = 'host'

# Binary message type codes
BIN_REGISTER_REQUEST: int = 1
BIN_REGISTER_RESPONSE: int = 2
BIN_ROUTING_UPDATE: int = 3
BIN_KEEP_ALIVE: int = 4
BIN_TOPOLOGY_UPDATE: int = 5

# Serialization functions

def serialize_register_request(switch_id: int, port: int) -> bytes:
    """Serialize REGISTER_REQUEST to binary format.
    Format: [1B type][4B switch_id][4B port]
    """
    return struct.pack('!Bii', BIN_REGISTER_REQUEST, switch_id, port)

def serialize_register_response(neighbors: List[NeighborInfo]) -> bytes:
    """Serialize REGISTER_RESPONSE to binary format.
    Format: [1B type][2B num_neighbors][for each: 4B id, 1B alive, 4B port, host as null-terminated]
    """
    data = struct.pack('!BH', BIN_REGISTER_RESPONSE, len(neighbors))
    for nbr in neighbors:
        host_bytes = nbr[KEY_HOST].encode('utf-8') + b'\x00'
        data += struct.pack('!iBi', nbr[KEY_NEIGHBOR_ID], 1 if nbr[KEY_ALIVE] else 0, nbr[KEY_PORT])
        data += host_bytes
    return data

def serialize_routing_update(routes: List[RoutingEntry]) -> bytes:
    """Serialize ROUTING_UPDATE to binary format.
    Format: [1B type][2B num_routes][for each: 4B sid, 4B did, 4B hop, 4B dist]
    """
    data = struct.pack('!BH', BIN_ROUTING_UPDATE, len(routes))
    for route in routes:
        data += struct.pack('!iiii', route[0], route[1], route[2], route[3])
    return data

# Deserialization functions

def deserialize_register_request(data: bytes) -> Tuple[int, int]:
    """Deserialize REGISTER_REQUEST from binary format.
    Returns: (switch_id, port)
    """
    _, switch_id, port = struct.unpack('!Bii', data[:9])
    return switch_id, port

def deserialize_register_response(data: bytes) -> List[NeighborInfo]:
    """Deserialize REGISTER_RESPONSE from binary format."""
    offset = 1  # Skip type byte
    num_neighbors = struct.unpack('!H', data[offset:offset+2])[0]
    offset += 2

    neighbors = []
    for _ in range(num_neighbors):
        nid, alive, port = struct.unpack('!iBi', data[offset:offset+9])
        offset += 9
        # Read null-terminated host string
        host_end = data.index(b'\x00', offset)
        host = data[offset:host_end].decode('utf-8')
        offset = host_end + 1

        neighbors.append({
            KEY_NEIGHBOR_ID: nid,
            KEY_ALIVE: bool(alive),
            KEY_HOST: host,
            KEY_PORT: port
        })

    return neighbors

def deserialize_routing_update(data: bytes) -> List[RoutingEntry]:
    """Deserialize ROUTING_UPDATE from binary format."""
    offset = 1  # Skip type byte
    num_routes = struct.unpack('!H', data[offset:offset+2])[0]
    offset += 2

    routes = []
    for _ in range(num_routes):
        sid, did, hop, dist = struct.unpack('!iiii', data[offset:offset+16])
        offset += 16
        routes.append([sid, did, hop, dist])

    return routes

def serialize_keep_alive(switch_id: int) -> bytes:
    return struct.pack('!Bi', BIN_KEEP_ALIVE, switch_id)

def deserialize_keep_alive(data: bytes) -> int:
    _, switch_id = struct.unpack('!Bi', data[:5])
    return switch_id

def serialize_topology_update(switch_id: int, neighbors: List[Tuple[int, bool]]) -> bytes:
    data = struct.pack('!BiH', BIN_TOPOLOGY_UPDATE, switch_id, len(neighbors))
    for nid, alive in neighbors:
        data += struct.pack('!iB', nid, 1 if alive else 0)
    return data

def deserialize_topology_update(data: bytes) -> Tuple[int, List[Tuple[int, bool]]]:
    offset = 1
    switch_id = struct.unpack('!i', data[offset:offset+4])[0]
    offset += 4
    num_neighbors = struct.unpack('!H', data[offset:offset+2])[0]
    offset += 2
    neighbors = []
    for _ in range(num_neighbors):
        nid, alive = struct.unpack('!iB', data[offset:offset+5])
        offset += 5
        neighbors.append((nid, bool(alive)))
    return switch_id, neighbors
