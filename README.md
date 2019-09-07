# Akka-project
Big Data streaming processing engine with end-to-end exactly-once delivery capability built on top of Akka as the final project for the *Middleware Technologies for Distributed Systems course @ Politecnico di Milano*.

# Overview
The engine accepts programs that define an arbitrary acyclic graph of operators that work with <Key, Value> pairs.
The key is a String and the Value can be of any(serializable) type.

The following graph operators are available:
- **Map**: takes in input a <Key, Value> pair and produces a <Key, Value> pair, according to a user-defined function.
- **FlatMap**: takes in input a <Key, Value> pair and produces a set of <Key, Value> pairs, according to a user-defined function.
- **Filter**: takes in input a <Key, Value> pair and either propagates it downstream or drops it, according to a user-defined predicate.
- **Aggregate**: accumulates n <Key, Value> pairs and produces a single <Key, Value> pair, according to a user-defined aggregate function.
- **Split**: forwards the incoming <Key, Value> pairs to multiple downstream subgraphs.
- **Merge**: accepts <Key, Value> pairs from multiple subgraphs and forwards them to the downstream operator in the main graph.

Input data is read by a single source operator and output data is consumed by a single sink operator.

# How it works
## Graph deploy and supervision
Each graph has its corresponding Master Node. The Master Node parses the user-defined graph and instantiates all the needed actors for the operators (more on this in the following *Operators and parallelism* section). The Master Node is itself an Akka actor and supervises the graph execution as it can be seen in the following picture.

<p align="center">
  <img width="70%" height="70%" src="https://i.imgur.com/RW4KmjP.png">
</p>

During the graph execution a consistent global state of the system is saved through a distributed snapshot initiated periodically by the Master Node. An eventual error occurring in one of the operators is propagated, thanks to the Akka hierarchy, to the Master Node, which restores the last valid snapshot. For more information regarding fault tolerance see the corresponding section below.

## Operators and parallelism
The Master Node spawns N actors for each operator according to the parallelism level (N = parallelism level). The data, flowing from the source to the sink, is hash-partitioned by Key among the different actors.
As an example, the previous graph definition with a parallelism level of 2 set on every operator leads to the following graph of actors supervised by the Master Node(which is not reported in the picture).

<p align="center">
  <img width="70%" height="70%" src="https://i.imgur.com/USPLAd0.png">
</p>

## Fault tolerance
The engine provides end-to-end exactly-once delivery for each graph even in the presence of failures, assuming that the graph's Master Node does not fail.
The engine does this by implementing the synchronous version of
[Lightweight Asynchronous Snapshots for Distributed Dataflows](https://arxiv.org/pdf/1506.08603.pdf) enriched to deal with Akka at-most-once delivery of messages(the engine can detect if a message is lost, because of Akka at-most-once delivery guarantee, and trigger a failure).
To guarantee end-to-end exactly-once delivery both the source and the sink must take part in the checkpointing mechanism. Both the source and the sink use text files and they both save, as their state, a file pointer they seek to know where to read/write. This is enough to mock a real-world scenario in which there is, for example, a Kafka source and a Cassandra sink taking part in the checkpointing.

## Stateful Operators
Besides the source and the sink there is another stateful operator, the Aggregate one, which saves the accumulated <Key, Value> pairs.
All the stateful operators take part in the checkpointing mechanism and write their state to disk. Note that also the remaining stateless operators take part in the checkpointing mechanism even though they don't save anything to disk. Therefore, it is easy to modify the existing stateless operators or to create new ones and make them stateful.


# Example usage
TODO
