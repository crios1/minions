# Sidecar Resources for Non-Python Libraries and Heavy CPU Work

Some tasks don’t fit comfortably inside a single Python interpreter: calling non-Python SDKs, running CPU-heavy algorithms, or isolating risky code. Minions lets you wrap a separate process as a `Resource`—a “sidecar”—so you can keep the main runtime stable while tapping external capabilities.

## Why use a sidecar?

- **Non-native libraries**: talk to JS/Java/Go SDKs that have no Python equivalent.  
- **CPU isolation**: bypass the GIL by pushing heavy compute into another process (multiprocessing, `subprocess`, or your own service).  
- **Fault containment**: crashes and exceptions in the sidecar won’t take down the Minions runtime; you handle IPC errors and retries instead.
- **Risk isolation**: if you need to run risky native code (e.g., a C/C++ extension that could SIGSEGV), keep it in the sidecar so a crash doesn’t kill the runtime—only the child process.

## Shape of the pattern

1. Create a `Resource` that starts and supervises a child process (e.g., `multiprocessing.Process`, `subprocess`, or a small HTTP/IPC server in another language).  
2. Expose async methods on the `Resource` that talk to the sidecar (IPC, HTTP, sockets, queues) and translate inputs/outputs.  
3. Handle errors, timeouts, and type conversions at the boundary; surface clean exceptions to your minions.  
4. Keep methods idempotent or retry-safe; the runtime can re-run steps after interruptions.

```python
import asyncio, json, subprocess
from minions import Resource

class PricingSidecar(Resource):
    async def startup(self):
        # Start the non-Python process (could be node, go, etc.)
        self.proc = await asyncio.create_subprocess_exec(
            "node", "pricing_sidecar.js",
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        )

    async def get_price(self, pair: str) -> float:
        payload = json.dumps({"pair": pair}) + "\n"
        assert self.proc.stdin and self.proc.stdout
        self.proc.stdin.write(payload.encode())
        await self.proc.stdin.drain()
        line = await asyncio.wait_for(self.proc.stdout.readline(), timeout=2)
        return json.loads(line)["price"]

    async def shutdown(self):
        if self.proc:
            self.proc.terminate()
            await self.proc.wait()
```

Swap out the IPC mechanism as needed (sockets, HTTP, gRPC, message queues); the `Resource` is your adapter and guardrail.

## Operational notes

- **Lifecycle**: start the sidecar in `startup`, tear it down in `shutdown`; let Gru manage ordering.  
- **Retries/timeouts**: treat IPC like any network call—timeouts, backoff, and idempotency guards.  
- **Resource sharing**: multiple minions can share the same sidecar `Resource`; Gru handles injection and reference counting.  
- **Portability**: keep sidecar binaries/scripts inside the project directory when possible (see {doc}`../concepts/portability`).  
- **Safety**: log and metric IPC failures; if the sidecar dies, decide whether to restart it or fail fast. For risky native code (C/C++/Rust extensions), run it in the sidecar so a SIGSEGV / SIGABRT only kills the child; detect that exit and restart or surface the failure cleanly.

## When to reach for this

- You need a JS/Java/Go SDK that doesn’t exist in Python.  
- You need multicore compute beyond what the GIL allows.  
- You want to isolate flaky or experimental code from the main runtime.  
- You’re integrating a legacy service but want a thin, testable boundary in Python.
