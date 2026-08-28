# Experimental runtime testing

This directory contains exploratory runtime probes that are not authoritative
contract or regression coverage. They may be developed with less review and
changed freely while investigating timing, concurrency, load, recovery, and
resource behavior.

When an experiment discovers a runtime defect, add the smallest useful,
reviewed regression to the ordinary test suite. Do not make production behavior
depend on experimental helpers or treat a passing experiment as the sole proof
of a user-facing guarantee.

Experimental code remains subject to the repository's linting and type checks.
Individual experiments document how they are invoked; they are not collected by
the default pytest run unless explicitly named.
