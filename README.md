# Akka-project
Big Data streaming processing engine with end-to-end exactly once delivery capability built on top of Akka as the final project for the *Middleware Technologies for Distributed Systems course @ Politecnico di Milano*.

# Overview
The engine accepts programs that define an arbitrary acyclic graph of operators which work with <Key, Value> pairs.
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
Each graph has its corresponding Master Node. The Master Node parses the user-defined graph and instantiates all the needed operators. The Master Node is itself an Akka actor which supervises all the actors needed for the operators, as can be seen in the following simpe graph.

<p align="center">
  <img width="70%" height="70%" src="https://i.imgur.com/RW4KmjP.png">
</p>

During the graph execution a consistent global state of the system is saved through a distributed snapshot initiated periodically by the Master Node. An eventual error occurring in one of the operators is propagated, thanks to the Akka hierarchy, to the Master Node, which restores the last valid snapshot.
For more information regarding fault tolerance see the corresponding section below.

The Master Node spawns N actors for each operator according the parallelism level (N = parallelism level). The data, flowing from the source to the sink, is hash-partitioned by Key among the different actors.

As an example, the previous graph definition with a parallelism level of 2 for every operator leads to the following graph of actors supervised by the Master Node(which is not reported in this picture)

<p align="center">
  <img width="70%" height="70%" src="https://i.imgur.com/USPLAd0.png">
</p>

## Fault tolerance
The engine provides end-to-end "exactly once" delivery for each graph even in the presence of failures, assuming that the  graph's Master Node does not fail.
The engine does this by implementing the synchronous version of
[Lightweight Asynchronous Snapshots for Distributed Dataflows](https://arxiv.org/pdf/1506.08603.pdf) enriched to deal with Akka at-most once delivery of messages(the engine is able to detect if a message is lost, because of akka at-most once delivery guarantee, and trigger a failure).
To guarantee exactly-once delivery both the source and the sink must take part in the snapshot mechanism. Both the source and the sink use text files and they both save, as their state, a file pointer they seek to know where to read/write. This is enough to mock a real-world scenario in which there is, for example, a Kafka source and a Cassandra sink taking part in the checkpointing. The other stateful operator which takes part in the snapshot by saving the accumulated <Key, Value> pairs is the Aggregate operator.

## Stateful Operators
As already mentioned, there are three operators with a state: the source, the sink and the aggregate operator. They take part in the checkpointing mechanism by writing their state to disk. Actually, also the other operators take part in the checkpointing by default even though they don't write anything to disk. Therefore it is easy to modify the existing operators or to create new ones and make them stateful.


# Example usage
TODO
