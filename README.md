# rocks-queue-java:A persistent queue based on RocksDB

RocksDB is an embedded KV database designed with write-ahead-log and log-structured-merge-tree. This project is a java version implementing persistent queue on RocksDB. It's very suitable for: 

- Applications want to persistent data on local disk for preventing data loss in memory if a crash.
- Client and server speed do not match, but usually in memory queue are bounded, `rocks-queue` provides unlimited storage capacity.

## How to implement queue on a KV store?

1. `RocksQueue` consists of `queue_name` and `_queue_name` two column families. `queue_name` column family for storing data, `_queue_name` for storing queue's `head` and `tail` pointer.
2. `RocksStore` is a rocks queue factory and maintaining the `<queue_name, RocksQueue>` relationship.

## Usage

#### Creating a rocks queue

```java
StoreOptions storeOptions = StoreOptions.builder().database("rocks_db").build();
                    
rocksStore = new RocksStore(storeOptions);
queue = rocksStore.createQueue(generateQueueName());
```  

#### Enqueue,Dequeue

```java
byte[] something = "something".getBytes();
long id = queue.enqueue(something);

QueueItem dequeue = queue.dequeue();
assertArrayEquals(dequeue.getValue(), something);
```

#### Consume, RemoveHead

You can also `consume` out the head of the queue and process it, and then invoke the `removeHead` method to delete it.

```java
QueueItem head = queue.consume();
log.info("Processing queue head {}", head);

queue.removeHead()
```

## JMX 

This project can expose JMX metrics for monitoring.  

#### `RocksStore`

Metric Name| Description
---|---
DatabaseName| RocksStore database name
RocksdbLocation | RocksStore location
RocksDBDiskUsageInBytes | The current size for RocksStore in bytes 
NumberOfQueueCreated | How many queues have been created in store
IsOpen | Is RocksStore been open
IsClosed | Is RocksStore been closed

#### `RocksQueue`

Metric Name| Description
---|---
QueueName | The queue name
QueueSize | Queue size
AccumulateBytes | The current size of the queue in bytes，enqueue will increase and dequeue decrease
HeadIndex | The head of the queue
TailIndex | The tail oft the queue
IsCreated | Has the queue been created
IsClosed | Has the queue been closed
SecondsSinceLastEnqueue | Seconds since the last enqueue in ms
SecondsSinceLastConsume | Seconds since the last consume in ms
SecondsSinceLastDequeue | Seconds since the last dequeue in ms

## Benchmark testing

Benchmark|                       Mode|  Cnt|        Score|        Error|  Units
---|---|---|---:|---:|:---:|
RocksQueueBenchmark.consume|     avgt|   50|    12576.618 |±   17929.697|  ns/op
RocksQueueBenchmark.dequeue|     avgt|   50|  2168917.940 |± 1063197.522|  ns/op
RocksQueueBenchmark.enqueue|     avgt|   50|  1762257.820 |±  232716.449|  ns/op
RocksQueueBenchmark.removeHead|  avgt|   50|  1558168.420 |±  276410.130|  ns/op