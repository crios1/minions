# Updating Minions

Use one of these operational flows when moving to a new Minions version.

## Preferred: stage then restart

1. Install/update the Minions package while your runtime is still running.
2. Perform a controlled restart to activate the new code.

This minimizes downtime to restart time only.

## Conservative: stop, install, start

1. Stop the runtime.
2. Install/update the Minions package.
3. Start the runtime.

Use this flow if you prefer a stricter maintenance window process.
