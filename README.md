# Gleam Asynchronous Gossip Protocol 
**Team Members: Aria Yousefi and Shane George Thomas**

### 1. Project Description
This is a distributed systems project in Gleam, that implements topology creation and message passing for the following topologies:
1) Full Network
2) Line
3) 3D Grid
4) Imperfect 3D Grid

The following are the algorithms supported by the program:
1) Gossip Protocol
2) Push Sum Algorithm 


### 2. Steps to run

``gleam run numNodes topology algorithm``

Where:
- numNodes is the number of actors involved
- topology is one of: full, 3D, line, or imp3D.
- algorithm is one of: gossip, push-sum.

Example usage: ``gleam run 1000 full push-sum``

### 3. Output Format

It prints the time taken by all actors to complete convergence for a given algorithm and a selected topology. 

Example output:

``Convergence completed in: 1361ms``

### 4. Upper limit and performance for each topology-algorithm combination

