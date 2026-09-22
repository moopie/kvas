# Kvas Design

Kvas is an in-memory key-value store consisting of a main writer service and 2 replicas
that can read the key-value pair but not add new values to the store.
Default port is `5757`

It consists of three commands:

- `GET <key>`
- `SET <key> <value>`
- `DEL <key>`

TcpServer uses JSON with a message size encoded in the first block to safely transfer network data.

The JSON data is stored in KV store as a string.

Example of a request: `'\x00\x00\x00\x2b{"command":"set","key":"foo","value":"bar"}'`

The server will return an empty value on a missing key, and will overwrite a key if it already exists.

# Replication details

Primary server listens on port `57570` for connections from replicas to send them data
to replicate.

When replica starts it connects to said port and listens for SET/DEL events from primary.

Additionally main server stores the write events in case a pelication instance becomes misaligned and disconnects.

# Guarantees

A client can rely for the data to be stored in the main server. Though the data will
eventually propagate to the replicas, it is not immediate.

# Concurrency and backpressure

`SET` and `DEL` can write only from a single thread.

Connection pool is limited to 128 clients per instance and a timeout of 2 seconds to limit pressure.
Every event that is sent to a replica has a distinct id to not reapply events accidentally sent more than once.

# Limitations

There are no tests for when the primary server disconnects, only in case of replica disconnections.

The data is stored in-memory, so if process dies/is restarted all data is lost.

