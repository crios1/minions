Runtime persistence serialization is implemented using `msgspec` MsgPack instead of Python's `json` module.

`msgspec` provides better performance and memory efficiency for binary encoding/decoding of types known in advance.

Since:
- users are encouraged to type the data structures they would like handled by the Minions runtime
- inflight workflow state (user type instances) is persisted as binary in SQLite by default

This project can naturally take advantage of the performance/memory advantages of `msgspec`.

Human-readable surfaces are separate:
- workflow/state persistence uses internal binary `msgspec` payloads
- file logs use JSONL text encoding
