#!/usr/bin/env python

"""This is the Switch Starter Code for ECE50863 Lab Project 1
Author: Xin Du
Email: du201@purdue.edu
Last Modified Date: December 9th, 2021
"""

import sys
import socket
import json
from datetime import date, datetime

# Please do not modify the name of the log file, otherwise you will lose points because the grader won't be able to find your log file
LOG_FILE = "switch#.log" # The log file for switches are switch#.log, where # is the id of that switch (i.e. switch0.log, switch1.log). The code for replacing # with a real number has been given to you in the main function.

# Those are logging functions to help you follow the correct logging standard

# "Register Request" Format is below:
#
# Timestamp
# Register Request Sent

def register_request_sent():
    log = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Register Request Sent\n")
    write_to_log(log)

# "Register Response" Format is below:
#
# Timestamp
# Register Response Received

def register_response_received():
    log = []
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

def routing_table_update(routing_table):
    log = []
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

def neighbor_dead(switch_id):
    log = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Neighbor Dead {switch_id}\n")
    write_to_log(log) 

# "Unresponsive/Dead Neighbor comes back online" Format is below:
#
# Timestamp
# Neighbor Alive <Neighbor ID>

def neighbor_alive(switch_id):
    log = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Neighbor Alive {switch_id}\n")
    write_to_log(log) 

def write_to_log(log):
    with open(LOG_FILE, 'a+') as log_file:
        log_file.write("\n\n")
        # Write to log
        log_file.writelines(log)

def register_with_controller(switch_id, controller_host, controller_port):
    # Create a UDP socket for communication with controller and other switches
    switch_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    switch_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    switch_socket.bind(('localhost', 0))  # Bind to any available port

    # Get the port that was assigned
    switch_port = switch_socket.getsockname()[1]

    print(f"Switch {switch_id} listening on port {switch_port} (UDP)")

    # Send Register Request to controller via UDP
    register_request = {
        'type': 'REGISTER_REQUEST',
        'switch_id': switch_id,
        'port': switch_port
    }

    switch_socket.sendto(
        json.dumps(register_request).encode(),
        (controller_host, controller_port)
    )
    register_request_sent()

    print(f"Switch {switch_id} sent Register Request to Controller")

    # Receive Register Response from controller
    data, addr = switch_socket.recvfrom(4096)
    response = json.loads(data.decode())

    if response['type'] == 'REGISTER_RESPONSE':
        register_response_received()
        neighbors = response['neighbors']

        print(f"Switch {switch_id} received Register Response")
        print(f"Neighbors: {neighbors}")

        return switch_socket, neighbors

    return None

def main():

    global LOG_FILE

    #Check for number of arguments and exit if host/port not provided
    num_args = len(sys.argv)
    if num_args < 4:
        print ("switch.py <Id_self> <Controller hostname> <Controller Port>\n")
        sys.exit(1)

    my_id = int(sys.argv[1])
    controller_host = sys.argv[2]
    controller_port = int(sys.argv[3])

    LOG_FILE = 'switch' + str(my_id) + ".log"

    # Register with controller and get neighbor information
    switch_socket, neighbors = register_with_controller(
        my_id, controller_host, controller_port
    )

    print(f"Switch {my_id} is running and connected to the network")

    # Keep the switch running
    # TODO: Implement neighbor discovery and routing protocols

if __name__ == "__main__":
    main()