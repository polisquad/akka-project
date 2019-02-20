# Akka Actors
Implement a **BigData (batch and stream) processing engine** using Akka actors. The engine accepts programs that define an arbitrary acyclic graph of operators. Operators take in input <Key, Value> pairs where both the Key and the Value are strings.

Implement the following operators:
Map: takes in input a <Key, Value> pair and produces a <Key, Value> pair, according to a user-defined function.
FlatMap: takes in input a <Key, Value> pair and produces a set of <Key, Value> pairs, according to a user-defined function.
Filter: takes in input a <Key, Value> pair and either propagates it downstream or drops it, according to a user-defined predicate.
Aggregate: accumulates n <Key, Value> pairs and produces a single <Key, Value> pair, according to a user-defined aggregate function.
Split: forwards the incoming <Key, Value> pairs to multiple downstream operators.
Merge: accepts <Key, Value> pairs from multiple operators and forwards them to the downstream operator.
The framework takes care of instantiating multiple workers (actors) to perform each operator in parallel on different data partitions (each worker is assigned a subset of the keys), and it handles the communication between operators.

Input data is made available by a single source node and output data is consumed by a single sink node. You can either assume that operators are instantiated for the entire duration of the computation, or that they are dynamically scheduled.

A master node supervises all the worker actors that execute the operators. The master, the source and the sink cannot fail. Furthermore, you can assume disks and read/write operations to disk to be non-faulty, while operators can fail at any time.

Implement fault-tolerance mechanisms to ensure (with low overhead) end-to-end exactly once delivery even in the presence of failures. Note: to ensure exactly once delivery, the computation must be deterministic: for instance, in the case of re-execution, a merge operator should re-process the input elements in the same order.

Implement a REST API to connect with the system, submit new jobs, and monitor the state of the platform.

> **Optional:** assume that the input client and the local disks are not reliable and use Apache Kafka to store input and output (and intermediate state, if needed).