# tests/assets_new

Deterministic, DSL-friendly assets for scenario tests.

Core primitives
- `event_counter.py`: `CounterEvent` with a monotonic `seq`.
- `pipeline_emit_1.py`: `Emit1Pipeline` emits one event after one subscriber joins.
- `pipeline_emit_1_a.py`, `pipeline_emit_1_b.py`, `pipeline_emit_1_c.py`: distinct one-event emitters for no-sharing scenarios.
- `pipeline_sync_2subs_2events.py`, `pipeline_sync_3subs_1event.py`: synchronization-focused emitters for deterministic shared/fanout scenarios.
- `pipeline_single_class.py`: single pipeline class without an explicit `pipeline` variable.
- `pipeline_resourced.py`: pipeline with a `FixedResource` dependency.
- `pipeline_dict_event.py`: pipeline that emits `dict` events.
- `resource_fixed.py`: `FixedResource` returns a stable value and accepts optional args.
- `resource_fixed_b.py`, `resource_fixed_c.py`: additional fixed resources for multi-resource scenarios.
- `minion_two_steps.py`: `TwoStepMinion` with two deterministic steps.
- `minion_two_steps_resourced.py`: `TwoStepResourcedMinion` uses `FixedResource`.
- `minion_two_steps_resourced_b.py`, `minion_two_steps_resourced_c.py`: resourced minions with distinct resource types.
- `minion_two_steps_resourced_shared_b.py`, `minion_two_steps_resourced_shared_c.py`: resourced minions that share `FixedResource`.
- `minion_two_steps_multi_resources.py`: minion depending on two resource types.
- `minion_uses_config.py`: `ConfigMinion` reads a config value into its workflow context.
- `minion_config_*.toml`: simple configs for multiple minion instances.

Failure-path assets
- `minion_fail_step.py`: raises a runtime error inside the first step.
- `minion_abort_step.py`: raises `AbortWorkflow` inside the first step.
- `resource_error.py`: resource method raises a runtime error.
- `minion_two_steps_resource_error.py`: calls `ErrorResource.explode()` in step 1.
- `pipeline_emit_error.py`: pipeline throws in `produce_event()`.

Invalid-definition assets
- `file_empty.py`: empty module (missing expected entrypoints).
- `file_with_two_minions.py`: two minion subclasses, no `minion` variable.
- `file_with_two_minions_and_explicit_minion.py`: explicit `minion` in a multi-minion module.
- `file_with_invalid_explicit_minion.py`: `minion` is not a Minion subclass.
- `file_with_two_pipelines.py`: two pipeline subclasses, no `pipeline` variable.
- `file_with_invalid_explicit_pipeline.py`: `pipeline` is not a Pipeline subclass.
- `minion_bad_context.py`: unserializable workflow context type.
- `minion_bad_event.py`: unserializable event type.
- `pipeline_unserializable_event.py`: unserializable event type for pipelines.
