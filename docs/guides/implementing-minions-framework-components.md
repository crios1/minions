# Writing Your Own Minions Framework Component Implementations: Logger, Metrics, and StateStore

Logger, Metrics, and StateStore are framework components Gru uses for logging,
metric collection, and workflow persistence. Most applications can use the
implementations Minions provides. Implement your own when you need behavior
those implementations do not offer.

Gru owns the lifecycle of the Logger, Metrics, and StateStore instances passed
to `Gru.create(...)`: it starts them during creation and shuts them down when
the `Gru` shuts down. Their supported lifecycle is that one `Gru` lifetime;
Minions does not require implementations to support startup or operations
after shutdown, so a later `Gru` uses new component instances. Persisted
workflow data resides in the backing store, and a replacement `StateStore` can
recover it when it uses a store containing that data.

## What to implement

- **Logger**: subclass `minions.interfaces.Logger` and implement its async
  `log(level, msg, **kwargs)` method. Keep `kwargs` as structured log context
  so framework fields remain queryable.
- **Metrics**: subclass `minions.interfaces.Metrics` and implement
  `increment_counter`, `set_gauge`, `observe_histogram`, and `snapshot`. Update
  methods receive a `MetricDefinition` and the labels and value for that
  operation. Stateful implementations must use one definition per metric name,
  support recording and snapshots before startup, and preserve those records
  through startup. To check a stateful implementation, add it to the
  implementations exercised by
  `tests/minions/_internal/_framework/test_metrics_contract.py` and run
  `pytest tests/minions/_internal/_framework/test_metrics_contract.py`.
- **StateStore**: subclass `minions.interfaces.StateStore`, call its
  constructor with the selected Logger and Metrics instances, and implement
  `save_context`, `delete_context`, `get_contexts_for_orchestration`, and
  `get_all_contexts`. Save and delete calls must return after the change is
  durable. The runtime supplies serialized context bytes; the store does not
  need to serialize workflow objects itself.

Pass the implementations to `Gru.create(...)`:

```python
gru = await Gru.create(
    logger=my_logger,
    metrics=my_metrics,
    state_store=my_state_store,
)
```

When supplying a StateStore, use the same Metrics instance for its constructor
and `Gru.create(metrics=...)`. The
{doc}`State and Persistence </concepts/state-and-persistence>` concept page
describes the StateStore contract and workflow durability in more detail.
