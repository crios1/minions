class UnsupportedUserCode(Exception):
    """
    Raised when user-submitted code uses invalid patterns or APIs
    that could compromise the framework's integrity or stability.
    """
    pass

class AbortWorkflow(Exception): # minion workflow
    """Raise this within a workflow step to abort it early."""
    ...

class MinionsError(Exception):
    "Exception used to bubble up context about framework exceptions"
    def __init__(self, message, *, context: dict | None = None):
        super().__init__(message)
        self.context = context or {}
