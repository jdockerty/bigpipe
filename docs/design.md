# Design

## Initial concepts

- Modelled after Kafka
  - Data is produced into `bigpipe` and is expired after a set amount of time (TTL) or from storage pressure.
- Simple replication as first form of distributed queue.


### Message format

From the sender:

```
key: string
value: bytes
```

When received by `bigpipe`, the structure is the same, but a timestamp and offset is included for ordering purposes:

```
key: string
value: bytes
timestamp: i64
offset: i64
```

### Writes

#### Log

After being received, the message is written to a log. This individual log is scoped/partitioned by the namespace name.

As such, all messages for a particular key are isolated within the same log.

A message's physical byte offset within the log is stored within an in-memory index for the read path.

### Reads

The messages within a log for a namespace are accessed by their logical offset, their position within the log.

This means that Messages are read from the log that resides on disk. An in-memory index is used to provide
fast access to the byte offset to seek to within the relevant segment file.


### Retention

In a similar manner to Kafka, either a time-to-live (TTL) or disk pressure causes segments to be deleted.

This happens to **closed segments only**.
