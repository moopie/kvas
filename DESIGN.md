# Kvas Design

Kvas is an in-memory key-value store consisting of a main writer service and 2 replicas
that can read the key-value pair but not add new values to the store.

It consists of three commands:

`GET <key>`
`SET <key> <value>`
`DEL <key>`

The server will return an empty value on a missing key, and willoverwrite a key if it already exists.

# Guarantess

A client can reply for the data to be stored in the main server. Though the data will
eventually propogate to the replicas, it is not immediate.

# Concurrency and backpressure

`SET` and `DEL` can be accessed only from a single thread.

Connection pool is limited to 128 clients and a timeout of 2 seconds to limit pressure.


