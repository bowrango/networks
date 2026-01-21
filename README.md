A Simple Software Defined Network

The Controller keeps track of the entire Switch network topology.

The `run_network.py` script starts multiple processes for the Controller and Switches. It takes a port number and network configuration
```
python3 run_network.py 9000 Config/graph_2.txt
```