#!/usr/bin/env python

"""
Automated Network Launcher (MacOS)
Starts the Controller and all Switches in separate terminal windows
"""

import sys
import os
import subprocess
import time

def parse_config(config_file):
    """Parse config file to determine number of switches"""
    with open(config_file, 'r') as f:
        lines = f.readlines()
        num_switches = int(lines[0].strip())
    return num_switches

def open_terminal(command, title):
    """Open a new Terminal window on macOS and execute command"""
    script = f'''
    tell application "Terminal"
        do script "cd {os.getcwd()} && {command}"
        set custom title of front window to "{title}"
        activate
    end tell
    '''
    subprocess.run(['osascript', '-e', script])

def main():
    if len(sys.argv) < 3:
        print("Usage: python run_network.py <controller_port> <config_file>")
        sys.exit(1)

    controller_port = int(sys.argv[1])
    config_file = sys.argv[2]

    if not os.path.exists(config_file):
        print(f"Error: Config file '{config_file}' not found")
        sys.exit(1)

    # Parse config to get number of switches
    num_switches = parse_config(config_file)

    print(f"Starting network with {num_switches} switches...")
    print(f"Controller port: {controller_port}")
    print(f"Config file: {config_file}")

    # Start controller
    controller_cmd = f"python controller.py {controller_port} {config_file}"
    print(f"\nStarting controller: {controller_cmd}")
    open_terminal(controller_cmd, "Controller")

    # Wait for controller to start
    time.sleep(1)

    # Start all switches
    for switch_id in range(num_switches):
        switch_cmd = f"python switch.py {switch_id} localhost {controller_port}"
        print(f"Starting switch {switch_id}: {switch_cmd}")
        open_terminal(switch_cmd, f"Switch {switch_id}")
        time.sleep(0.5)

    print(f"\nNetwork started successfully!")
    print(f"- 1 Controller terminal")
    print(f"- {num_switches} Switch terminals")
    print("\nTo stop the network, close all terminal windows or press Ctrl+C in each")

if __name__ == "__main__":
    main()
