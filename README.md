# CaesiumDB ⚛️💫
**CaesiumDB** is a lightweight, high-performance in-memory data structure store built in C++, implementing the Redis protocol. Drawing its name from Caesium (Cs)—the element that defines the second in atomic clocks—this database delivers the same precision to data operations: fast, accurate, and absolutely reliable.

## About

CaesiumDB is a complete Redis-compatible server implementation that supports strings, lists, streams, transactions, and master-replica replication. Built from scratch in C++, it demonstrates modern systems programming techniques including asynchronous I/O, concurrent client handling, and binary protocol parsing.

This project successfully passes all stages of the [CodeCrafters Redis Challenge](https://codecrafters.io/challenges/redis), implementing production-ready features like:
- Multi-client concurrent connection handling
- RESP (Redis Serialization Protocol) encoding/decoding
- Master-replica replication with handshake protocol
- Transaction support with isolation
- Blocking operations with timeout support
- Stream data structures for event sourcing

## Features

### Core Data Structures
- **Strings**: Basic key-value storage with expiration support
- **Lists**: Doubly-linked lists with head/tail operations
- **Streams**: Append-only logs with time-based IDs for event streaming

### Commands Supported

#### Basic Commands
- `PING` - Test server connectivity
- `ECHO <message>` - Echo the given string

#### String Commands
- `SET <key> <value> [PX milliseconds]` - Set a key with optional expiration
- `GET <key>` - Retrieve value by key
- `TYPE <key>` - Get the data type of a key

#### List Commands
- `RPUSH <key> <element> [element ...]` - Append elements to list
- `LPUSH <key> <element> [element ...]` - Prepend elements to list
- `LPOP <key> [count]` - Remove and return elements from list head
- `LRANGE <key> <start> <stop>` - Get range of elements from list
- `LLEN <key>` - Get list length
- `BLPOP <key> [key ...] <timeout>` - Blocking pop with timeout

#### Stream Commands
- `XADD <key> <ID> <field> <value> [field value ...]` - Add entry to stream
- `XRANGE <key> <start> <end>` - Query stream entries in range
- `XREAD [BLOCK milliseconds] STREAMS <key> [key ...] <ID> [ID ...]` - Read from streams

#### Transaction Commands
- `MULTI` - Start transaction block
- `EXEC` - Execute queued commands atomically
- `DISCARD` - Discard transaction
- `INCR <key>` - Increment integer value (transaction-safe)

#### Replication Commands
- `INFO REPLICATION` - Get replication status
- `REPLCONF <option> <value>` - Configure replication parameters
- `PSYNC <replicationid> <offset>` - Initiate replication sync

### Advanced Features

#### Expiration Support
Keys can have TTL (time-to-live) set in milliseconds using the `PX` option in `SET` commands. Expired keys are automatically cleaned up on access.

#### Master-Replica Replication
- Full replication handshake protocol
- RDB file transfer for initial sync
- Command propagation to replicas
- Replica-aware write operations

#### Transaction Isolation
- Commands queued during `MULTI` are executed atomically
- Rollback on syntax errors
- Per-client transaction state management

#### Blocking Operations
- `BLPOP` with timeout support (0 for infinite)
- Event notification system for blocked clients
- Non-blocking operations for normal commands

#### Stream Auto-ID Generation
- Fully automatic IDs using `*`
- Partially automatic IDs with sequence numbers
- Monotonic ID validation

## Implementation Details

### Architecture
- **Single-threaded event loop** for handling multiple concurrent clients
- **Non-blocking I/O** using socket operations
- **RESP protocol parser** for Redis wire protocol compatibility
- **Hash map storage** for O(1) key lookups
- **Deque-based lists** for efficient head/tail operations
- **Ordered map streams** for time-based queries

### Key Components
- `main.cpp` - Server initialization, connection handling, command routing
- `encode.cpp/h` - RESP protocol encoding
- `decode.cpp/h` - RESP protocol decoding and command parsing
- Built with CMake and vcpkg for dependency management

### Protocol Support
- RESP (Redis Serialization Protocol) v2
- Binary-safe strings
- Bulk strings, arrays, integers, simple strings, and errors
- Nested array support for complex responses

## Building and Running

### Prerequisites
- CMake 3.10 or higher
- C++17 compatible compiler (GCC, Clang, or MSVC)
- vcpkg (automatically configured)

### Build
```sh
cmake -B build -S .
cmake --build build
```

### Run as Master
```sh
./your_program.sh
# Server starts on port 6379
```

### Run as Replica
```sh
./your_program.sh --port 6380 --replicaof "localhost 6379"
```

### Test with redis-cli
```sh
redis-cli PING
redis-cli SET mykey "Hello CaesiumDB"
redis-cli GET mykey
redis-cli XADD stream * temperature 25 humidity 60
```

## Development

### Running Tests
```sh
git push origin master
# Tests run automatically on CodeCrafters platform
```

### Adding New Commands
1. Add command handler in `main.cpp`
2. Implement encoding in `encode.cpp` if needed
3. Update command routing logic
4. Test with redis-cli

## License

This project was built as part of the CodeCrafters Redis Challenge.

## Acknowledgments

Built as part of the [CodeCrafters](https://codecrafters.io) "Build Your Own Redis" challenge, demonstrating proficiency in systems programming, network protocols, and database internals.
