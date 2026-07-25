from collections.abc import Mapping


class UnsupportedUserCode(Exception):
    """
    Raised when user-submitted code uses invalid patterns or APIs
    that could compromise the framework's integrity or stability.
    """
    pass


class AbortWorkflow(Exception):  # minion workflow
    """Raise this within a workflow step to abort it early."""
    ...


class MinionsError(Exception):
    "Exception used to bubble up context about framework exceptions"
    def __init__(
        self,
        message: str,
        *,
        context: Mapping[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.context: dict[str, object] = dict(context or {})


class TaskCancellationError(Exception):
    """Base exception for failures while cancelling runtime-owned tasks."""


class TaskCancellationTimeoutError(TaskCancellationError, TimeoutError):
    """A runtime-owned task did not finish within its cancellation deadline."""

    def __init__(self, label: str, timeout: float) -> None:
        self.label = label
        self.timeout = timeout
        super().__init__(
            f"Timeout while cancelling task {label!r} after {timeout:g} seconds"
        )


class TaskCancellationErrors(TaskCancellationError):
    """One or more runtime-owned tasks could not be cancelled cleanly."""

    def __init__(self, errors: list[Exception]) -> None:
        self.errors = tuple(errors)
        details = "; ".join(str(error) for error in errors)
        super().__init__(
            f"{len(errors)} task cancellation error(s)"
            + (f": {details}" if details else "")
        )
