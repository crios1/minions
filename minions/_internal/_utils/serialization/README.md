Serialization is implemented using `msgspec` instead of Python's `json` module.

`msgspec` provides better performance and memory efficiency for binary encoding/decoding of types known in advance.

Since:
- users are encouraged to type the data structures they would like handled by the Minions runtime
- inflight workflow state (user type instances) is persisted as binary in SQLite by default

This project can naturally take advantage of the performance/memory advantages of `msgspec`.
