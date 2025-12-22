# SAC_Mutex
Implementation of Maekawa's mutual exclusion algorithm for distributed systems.

Mutual exclusion assignment for Sistemes Actuals de Computació

---

## Use

Tu run this simulation simply run the `main.py` script on a terminal:

```bash
Python -B main.py
```

or

```bash
python3 main.py
```

> ⚠️ The number of nodes in the distributed systems may be selected by the user manually changing the value for the variable `numNodes` in the file `config.py`. Mind that the algorithm might not work for non-perfect square number of nodes or very large ones.

---

## Introduction

This repository contains the code for a simulation of several **distributed nodes** that want to access a shared resource, using **Maekawa's algorithm for mutual exclusion** by which they exchange messages to decide who enters the critical section. Each node belongs in a ***quorum***, formed by the only nodes with the node communicates with; a node can only enter the critical section when all his peers in the *quorum* has granted it permission. For the algorithm to work properly, some conditions must be met:

- The intersection between any two *quora* must not be empty.
- All *quora* must contain exactly the same number of nodes.
- If *K* is the number of nodes in a *quorum*, each node must belongs exactly in *K quora*.

Each node has a certain **priority** determined by two factors:

1. The **Lamport timestamp**: incremented on each event, i.e. for each message sent or received, it marks the time ordering of the distributed system and is used as the **main criterium for priority**.
2. In case of **tie**, some other **secondary criteria** must be held into account to resolve the priority.

There are **six** kinds of messages:

1. **Request:** A node asks all other nodes in its *quorum* permission for entering the critical section.
2. **Grant:** A node replies to a request by granting its permission.
3. **Failed:** A node replies to a request by denying its permission, due to some other node with higher priority being already granted.
4. **Release:** A node notifies all other nodes in its *quorum* that it leaves the critical section.
5. **Inquire:** Upon receiving a request, if the node has already granted permission to some lower priority node it asks this node if it has gotten into the critical section.
6. **Yield:** Upon receiving an inquire, a node that hasn't entered the critical section yet answers that it gives up its attempt.

---

## Design decisions

For the development of this simulation the following decisions have been made:

- **Programming language:** Since the project is built on a skeleton provided by the professor, the code is written entirely in **Python** following the base. The only files modified are `node.py`, `nodeServer.py` and `message.py`. Additionally, the file `logger_config.py` has been added to provide a simple logging system, allowing the user to select from `info` and `debug` modes in the code.
- **Quorum formation:** If all nodes are arranged in a square grid, ordered by their IDs, a *quorum* for any node is formed by the nodes on the same row and column. This decision conditions that the number of nodes in the system should be a perfect square such as 4, 9, 16, etc.
- **Priority tie break:** Each node has a numerical attribute to uniquely identify it, which serves as **second criterium for priority** in case of tie with the Lamport timestamps.
- **Data structures:** To manage control information during the message exchange, the following data structures have been used:
    - The **queue** of nodes wanting to access the critical section is implemented with a `PriorityQueue`, since this structure is already designed to order its elements by priority.
    - To handle the **grants sent** by a node, a `tuple` has been considered enough to store the current highest priority node's Lamport timestamp and ID.
    - To handle the **grants received** by a node, a `set` is used to contain the IDs of the granting nodes, since it ensures non-repetition.
    - Three `boolean` variabes are used to control whether a node has yielded, has been failed or has gotten into the critical section.
- **Yield supposition:** In order to reduce deadlock probability, whenever a node receives an **inquire** message replies back with a **yield** if it hasn't gotten into the critical section.
- **Condition variables:** The shared variables mentioned above are protected with Python's `Condition` variables to ensure mutual exclusion.
- **Random spawn delay:** Since Maekawa's algorithm is not completely deadlock free, a time span of serveral seconds has been established where the nodes begin at random times. The span increases with the number of nodes, because more nodes make deadlock even more likely.

---

## Communication process

### Architecture

As has already been mentioned, the core component in this simulation is the **node**, represented by the `Node` class in the `node.py` file. A node features a **queue** for the nodes that are waiting for its grant, keeps track of the grants sent and received and another control info such as its Lamport timestamp and failed, yield or in critical section conditions. The **system** consists of serveral nodes that exchange messages.

The **message exchange** is handled via threads, where each node features both a **client thread** for handling the requests and a **server thread** for the replies. This communication is directly built on **sockets**.

A **message** is represented by the class `Message` in the `message.py` file. It contains the following information:
- The **message type**, one of the six listed in the introduction (request, grant, failed, etc).
- The **message source**, being the ID of the node who sent it.
- The **message destination**, being the ID of the node who received it.
- The **Lamport timestamp**, representing the logical timestamp at which the message is sent.
- The **message payload**, which is often ignored in this simulation.

The messages are converted to `json` format for their exchange and converted back to `Message` for their processing.

---

### Simulation

The simulation follows these steps:

1. When a node wakes up at a random time, sends a **request** to all its *quorum* peers.
2. The node **waits for all its peers to reply with a grant**. This wait may be arbitrarily long, since the nodes will assert if permissions are granted or denied.
3. Once all grants have been received, the node **enters the critical section**.
4. The node **exits the critical section** and sends a **release** to all its *quorum* peers.

The simulation ends when all nodes have entered the critical section twice.

---

## Final considerations

Maekawa's algorithm **is not deadlock free** and can run into **circular waits**, specially when many nodes want to access the critical section simultaneously. This makes **scalability** difficult, because the greater the number of nodes, the greater the chance of many nodes wanting to access the critical node at the same time. To **mitigate this issue**, in this simulation the time span for the nodes to randomly start has been set quite broad and proportional to the total of nodes in the system. However, in a real scenario the behavior of the nodes would likely be unpredictable and deadlocks might occur.

On the other side, Maekawa's algorithm **greatly reduces the number of messages** exchanged with respect to other mutual exclusion algorithms for distributed systems such as Ricard-Agrawala's.

