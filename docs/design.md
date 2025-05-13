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

When received by `bigpipe`, the structure is the same, but a timestamp is included for ordering purposes:

```
key: string
value: bytes
timestamp: i64
```
