# Akka-project
Big Data streaming processing engine built on top of Akka.
The engine accepts programs that define an arbitrary acyclic graph of operators. Operators works with <Key, Value> pairs.

The following graph operators are available:
- **Map**: takes in input a <Key, Value> pair and produces a <Key, Value> pair, according to a user-defined function.
- **FlatMap**: takes in input a <Key, Value> pair and produces a set of <Key, Value> pairs, according to a user-defined function.
- **Filter**: takes in input a <Key, Value> pair and either propagates it downstream or drops it, according to a user-defined predicate.
- **Aggregate**: accumulates n <Key, Value> pairs and produces a single <Key, Value> pair, according to a user-defined aggregate function.
- **Split**: forwards the incoming <Key, Value> pairs to multiple downstream subgraphs.
- **Merge**: accepts <Key, Value> pairs from multiple subgraphs from a split and forwards them to the downstream operator in the main graph.

The engine spawns n workers(actors) for each operator according to its parallelism (number of workers = parallelism level). Data is hash-partitioned by key among the different workers.

Input data is read by a single source operator and output data is consumed by a single sink operator.

All the operators are instantiated for the entire duration of the computation.

The engine provides End-to-end "exactly once" delivery even in the presence of failures, assuming that the Master Node, which supervises the graph execution, does not fail.
The engine does this by implementing the synchronous version of
[Lightweight Asynchronous Snapshots for Distributed Dataflows](https://arxiv.org/pdf/1506.08603.pdf) enriched to deal with Akka at-most once delivery of messages.
To guarantee exactly-once delivery both the source and the sink must take part in the snapshot mechanism. Both the source and the sink use text files and they both save, as their state, a file pointer they seek to know where to read/write. This is enough to mock a real-world scenario in which there is, for example, a Kafka source and a Cassandra sink taking part in the checkpointing.

A REST API is provided to submit new jobs and monitor their execution.
