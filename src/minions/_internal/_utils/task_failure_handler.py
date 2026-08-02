import sys
from collections.abc import Awaitable
from typing import Protocol


class TaskFailureHandler(Protocol):
    """Handle a swallowed task failure synchronously or asynchronously."""

    def __call__(
        self, exception: BaseException, task_name: str | None, /
    ) -> Awaitable[None] | None: ...


def report_task_failure_to_stderr(
    exception: BaseException,
    task_name: str | None,
) -> None:
    """A TaskFailureHandler that reports failures directly to stderr."""
    task_label = f" ({task_name})" if task_name else ""
    print(
        f"[asyncio task failed]{task_label}: {type(exception).__name__}: {exception}",
        file=sys.stderr,
    )
